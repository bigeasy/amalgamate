const assert = require('assert')
const events = require('events')
const noop = require('nop')

const { Player, Recorder } = require('transcript')

const path = require('path')
const fs = require('fs').promises

const Future = require('perhaps')

const Amalgamator = require('./amalgamate')

const WriteAhead = require('writeahead')
const Magazine = require('magazine')
const FileSystem = require('b-tree/filesystem')
const WriteAheadOnly = require('b-tree/writeahead')

function _unshift (groups, group, version) {
    groups.unshift({
        state: 'appending',
        group: group,
        // TODO We need rules for min and max. Currently `min` is exclusive
        // and `max` is inclusive. Either both exclusive or inclusive,
        // exclusive like `slice()`.
        min: version,
        max: version,
        mutations: new Map,
        rotated: new Set,
        amalgamated: new Set,
        unstaged: new Set,
        references: [ 0, 0 ],
        heft: 0
    })
}

class Locker extends events.EventEmitter {
    constructor (destructible, { writeahead, size, checksum = () => '0', groups }, strata) {
        super()
        this.destructible = destructible
        this.destroyed = false
        this._size = [{ max: size }]
        this._group = groups[0].group + 1
        this._version = groups[0].max + 1
        this._order = 1
        this._completed = 0
        this._amalgamators = new Map
        this._groups = groups
        this._unstaged = []
        this._references = 0
        this._zeroed = new Future({ resolution: [] })
        this._rotated = new Future({ resolution: [] })
        this._writeahead = writeahead
        this._recorder = Recorder.create(checksum)
        this._strata = {
            primary: strata,
            stage: {
            }
        }
    }

    static async open (writeahead, { create = false, size = 1024 * 1024, checksum = () => '0' } = {}) {
        const groups = []
        _unshift(groups, 1, 1)
        const player = new Player(checksum)
        for await (const buffer of writeahead.get('commit')) {
            for (const entry of player.split(buffer)) {
                const version = JSON.parse(entry.parts.shift())
                const mutation = { version, completed: 0, rolledback: false }
                groups[0].mutation.set(version, mutation)
                groups[0].max = Math.max(groups[0].max, version)
            }
        }
        return { writeahead, size, checksum, groups }
    }

    async open (destructible, { directory, handles, create, key, serializer, checksum, extractor }, options) {
        const open = {
            key: key,
            directory: directory,
            storage: await FileSystem.open({ handles, directory, create, serializer, checksum, extractor }),
            stages: [],
            options: {
                serializer: {
                    key: {
                        serialize: (key) => {
                            const [ value, version, order ] = key
                            assert(value != null)
                            const header = { version, order }
                            const buffer = Buffer.from(JSON.stringify(header))
                            return [ buffer ].concat(serializer.key.serialize(value))
                        },
                        deserialize: (parts) => {
                            const { version, order } = JSON.parse(parts[0].toString())
                            return [
                                serializer.key.deserialize(parts.slice(1)),
                                version, order
                            ]
                        }
                    },
                    parts: {
                        serialize: (parts) => {
                            const header = Buffer.from(JSON.stringify(parts[0]))
                            if (parts[0].method == 'insert') {
                                return [ header ].concat(serializer.parts.serialize(parts.slice(1)))
                            }
                            return [ header ].concat(serializer.key.serialize(parts[1]))
                        },
                        deserialize: (parts) => {
                            const header = JSON.parse(parts[0].toString())
                            if (header.method == 'insert') {
                                return [ header ].concat(serializer.parts.deserialize(parts.slice(1)))
                            }
                            return [ header ].concat(serializer.key.deserialize(parts.slice(1)))
                        }
                    }
                },
                extractor: (parts) => {
                    if (parts[0].method == 'insert') {
                        return [
                            extractor(parts.slice(1)),
                            parts[0].version,
                            parts[0].order
                        ]
                    }
                    return [ parts[1], parts[0].version, parts[0].order ]
                }
            }
        }
        for await (const entry of WriteAheadOnly.wal.iterator(this._writeahead, key, 'stage')) {
        }
        if (open.stages.length == 0) {
            open.stages.push({
                name: `stage.${this._groups[0].group}`,
                groups: [ this._groups[0].group ],
                count: 0,
                key: this._groups[0].group,
                storage: await WriteAheadOnly.open({
                    ...open.options,
                    writeahead: this._writeahead,
                    key: [ key, 0 ],
                    create: [ [ 'locate' ], key ]
                })
            })
        }
        const amalgamator = new Amalgamator(destructible, this, open, options)
        this._amalgamators.set(amalgamator, { key, options: open.options })
        return amalgamator
    }

    check () {
        this._maybeRotate()
    }

    recover (versions) {
        assert(this._groups[0].group == 1 && this._groups[0].heft == 0)
        for (const [ version, committed ] of versions) {
            const mutation = {
                version, completed: 0, rolledback: ! committed
            }
            this._groups[0].mutations.set(version, mutation)
            this._groups[0].max = Math.max(this._groups[0].max, version)
            this._version = this._groups[0].max + 1
        }
    }

    snapshot () {
        this._references++
        assert(this._shutdown == null)
        const groups = this._groups.filter((group, index) => {
            return index == 0 || group.amalgamated.size != this._amalgamators.size
        })
        groups.forEach((group, index) => group.references[index]++)
        return { groups, completed: this._completed  }
    }

    mutator () {
        assert(this._shutdown == null)
        // The mutation with its nested `completed` that is uncompleted,
        // therefore `MAX_SAFE_INTEGER`, not to be confused with the one we're
        // merging from the `snapshot()`.
        const mutation = {
            version: this._version++,
            order: 0,
            completed: Number.MAX_SAFE_INTEGER,
            rolledback: false
        }
        this._groups[0].max = mutation.version
        this._groups[0].mutations.set(mutation.version, mutation)
        return { ...this.snapshot(), mutation, conflicted: false }
    }

    visible (version, { completed, mutation: current = null }) {
        if (
            version == 0 ||
            version == Number.MAX_SAFE_INTEGER ||
            (current != null && current.version == version)
        ) {
            return true
        }
        const group = this._groupByVersion(version)
        const mutation = group.mutations.get(version)
        assert(mutation)
        return mutation.completed <= completed && ! mutation.rolledback
    }

    // Should be called immediately before calling `commit`.
    conflicted (version, { completed, mutation: { version: current } }) {
        if (current == version) {
            return false
        }
        const group = this._groupByVersion(version)
        const mutation = group.mutations.get(version)
        assert(mutation)
        if (mutation.completed > completed && ! mutation.rolledback) {
            mutation.conflicted = true
            return true
        }
        return false
    }

    // Our write will be synchronous as will our in-memory commit. If we do not
    // actually write our commit, if the append fails to write, any commit that
    // follows this commit will also fail to write.
    commit ({ mutation, groups }) {
        const promise = this._writeahead.write([{
            keys: [ 'commit' ],
            buffer: (this._recorder)([[ Buffer.from(JSON.stringify(mutation.version)) ]])
        }])
        mutation.completed = this._completed = this._order++
        this.release({ groups })
        return promise
    }

    rollback ({ mutation, groups }) {
        mutation.completed = this._completed = this._order++
        mutation.rolledback = true
        this.release({ groups })
    }

    get status () {
        return {
            heft: {
                max: this._heft.max,
                total: this._groups.reduce((sum, group) => sum + group.heft, 0)
            },
            version: this._version,
            group: this._group,
            completed: this._completed,
            amalgamators: this._amalgamators.size,
            groups: this._groups.map(group => {
                const mutations = []
                for (const mutation of group.mutations.values()) {
                    mutations.push(mutation)
                }
                return {
                    ...group, mutations,
                    rotated: group.rotated.size,
                    amalgamated: group.amalgamated.size,
                    unstaged: group.unstaged.size
                }
            })
        }
    }

    _maybeRotate () {
        if (! this.destroyed &&
            this._groups.length == 1 &&
            this._groups[0].state == 'appending' &&
            this._size[0].max < this._writeahead.position
        ) {
            this.destructible.ephemeral($ => $(), 'rotate', this._rotate())
        }
    }

    _groupByVersion (version) {
        return this._groups.filter(group => group.min < version && version <= group.max)[0]
    }

    group (version) {
        return this._groupByVersion(version).group
    }

    _maybeAmalgamate () {
        if (
            this._groups.length == 2 &&
            this._groups[1].state == 'rotated' &&
            this._groups[1].references[0] == 0
        ) {
            this._amalgamate.resolve()
        }
    }

    _maybeUnstage () {
        if (
            this._groups.length == 2 &&
            this._groups[1].state == 'amalgamated' &&
            this._groups[1].references[1] == 0
        ) {
            this._unstage.resolve()
        }
    }

    unstaged (amalgamator) {
        assert(!this._groups[1].unstaged.has(amalgamator))
        this._groups[1].unstaged.add(amalgamator)
        if (this._groups[1].unstaged.size == this._amalgamators.size) {
        }
    }

    release ({ groups }) {
        groups.forEach((group, index) => group.references[index]--)
        this._maybeAmalgamate()
        this._maybeUnstage()
        if (--this._references == 0) {
            this._zeroed.resolve()
        }
    }
    //

    // Called by amalgamators when amalgamator heft changes. Heft only ever
    // increases since the amalgamators are write-ahead logs.

    //
    heft (version, heft) {
        this._groupByVersion(version).heft += heft
        this._maybeRotate()
    }
    //

    // The heft property of the locker is an array and we temporarily shift a
    // negative maximum heft onto the array to force a rotate the next time we
    // check. If we are currently rotating we will immediately rotate when
    // finished. The `done` future is separate form the ones used to drain.

    //
    rotate () {
        if (this._amalgamators.size != 0) {
            const done = new Future
            this._heft.unshift({ max: -1, done })
            this._maybeRotate()
            return done.future
        }
    }

    _drains () {
        const drains = []
        if (this._references != 0) {
            if (this._zeroed.fulfilled) {
                this._zeroed = new Future
            }
            this._zeroed.promise.name = 'zeroed'
            drains.push(this._zeroed.promise)
        }
        if (this._groups.length != 1 || this._groups[0].state != 'appending') {
            if (this._rotated.fulfilled) {
                this._rotated = new Future
            }
            this._rotated.promise.name = 'rotated'
            drains.push(this._rotated.promise)
        }
        return drains
    }

    drain () {
        let drains = this._drains()
        if (drains.length != 0) {
            return (async () => {
                do {
                    for (const promise of drains) {
                        await promise
                    }
                    drains = this._drains()
                    console.log('drain loop', drains.length, this._groups)
                } while (drains.length != 0)
            }) ()
        }
        return null
    }

    async _rotate () {
        while (this._size.length != 1) {
            this._unstaged.push(this._size.shift())
        }
        this._groups[0].state = 'rotating'
        await this._writeahead.rotate()
        for (const [ amalgamator, { key, options } ]  of this._amalgamators) {
            const stage = {
                name: `stage.${this._group}`,
                groups: [ this._group ],
                count: 0,
                key: this._group,
                storage: await WriteAheadOnly.open({
                    ...options,
                    writeahead: this._writeahead,
                    key: [ key, this._group ],
                    create: [ [ 'locate' ], key ]
                })
            }
            amalgamator.rotate(stage)
        }
        this._groups[0].state = 'rotated'
        this._groups[0].max = this._version
        _unshift(this._groups, this._group++, this._version)
        this._version++
        this._amalgamate = new Future
        this._maybeAmalgamate()
        await this._amalgamate.promise
        this._groups[1].state == 'amalgamating'
        // TODO Is this okay? No one is editing.
        const mutator = { completed: this._completed  }
        for (const amalgamator of this._amalgamators.keys()) {
            await amalgamator.amalgamate(mutator)
        }
        this._groups[1].state = 'amalgamated'
        this._unstage = new Future
        this._maybeUnstage()
        await this._unstage.promise
        this._groups[1].state = 'unstaging'
        for (const amalgamator of this._amalgamators.keys()) {
            this.destructible.progress()
            amalgamator.unstage()
        }
        const group = this._groups.pop()
        this.emit('amalgamated', group.min, group.max)
        while (this._unstaged.length != 0) {
            this._unstaged.shift().done.resolve()
        }
        this._maybeRotate()
        this._rotated.resolve()
        console.log('here', this._groups[0].group, this._group)
    }
}

module.exports = Locker
