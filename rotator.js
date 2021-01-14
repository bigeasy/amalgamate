const assert = require('assert')

const { Recorder, Player } = require('transcript')

const FileSystem = require('b-tree/filesystem')
const WriteAheadOnly = require('b-tree/writeahead')
const Amalgamator = require('./amalgamate')

const Locker = require('./locker')
const Future = require('perhaps')
//

// Rotator and Locker are separate so we can unit test the MVCC locking logic
// synchrnously, separately.

//
class Rotator {
    // **TODO** No need to double checksum the write-ahead log. Starting to feel
    // like the write-ahead log should return an array of buffers, it should do
    // the partitioning for you, because we're doing it twice. Once in the log
    // and once it comes out of the log.
    constructor (destructible, { writeahead, mutations, version }, { size = 1024 * 1024 } = {}) {
        this.locker = new Locker({ mutations, version, size })
        this.destructible = destructible
        this.destructible.destruct(() => this.locker.destroy())
        this._amalgamators = new Map
        this._writeahead = writeahead
        this._zeroed = Future.resolve()
        this._rotated = Future.resolve()
        this._recorder = Recorder.create(() => '0')
        const rotator = this.destructible.durable($ => $(), 'rotate', this._rotate())
        writeahead.deferrable.increment()
        destructible.destruct(() => {
            destructible.ephemeral('shutdown', async () => {
                await rotator.done
                await destructible.copacetic($ => $(), null, async () => this.drain())
                writeahead.deferrable.decrement()
            })
        })
    }

    static async open (writeahead, { create = false } = {}) {
        const player = new Player(() => '0')
        const mutations = new Map
        let version = 0
        for await (const buffer of writeahead.get('commit')) {
            for (const entry of player.split(buffer)) {
                const mutation = {
                    version: JSON.parse(entry.parts.shift()),
                    completed: 0,
                    rolledback: false
                }
                mutations.set(mutation.version, mutation)
                version = Math.max(version, mutation.version)
            }
        }
        return { writeahead, mutations, version }
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
        for await (const entries of WriteAheadOnly.wal.iterator(this._writeahead, [ 'locate' ], key)) {
            for (const entry of entries) {
                const key = entry.header.value
                const storage = await WriteAheadOnly.open({ writeahead: this._writeahead, key, ...open.options })
                console.log('hello')
                open.stages.push({
                    name: `stage.${key[1]}`,
                    group: this.locker.latest,
                    count: 0,
                    appending: false,
                    storage: storage
                })
            }
        }
        open.stages.reverse()
        if (open.stages.length == 0) {
            console.log('creating')
            open.stages.push({
                name: `stage.${this.locker.latest}`,
                group: this.locker.latest,
                count: 0,
                key: this.locker.latest,
                appending: true,
                storage: await WriteAheadOnly.open({
                    ...open.options,
                    writeahead: this._writeahead,
                    key: [ key, 0 ],
                    create: [ [ 'locate' ], key ]
                })
            })
        } else {
            open.stages[0].appending = true
        }
        const amalgamator = new Amalgamator(destructible, this, open, options)
        this._amalgamators.set(amalgamator, { key, options: open.options })
        return amalgamator
    }

    advance () {
        this.locker.advance(this._writeahead.position)
    }

    commit (mutation) {
        const future = this._writeahead.write([{
            keys: [ 'commit' ],
            buffer: (this._recorder)([[ Buffer.from(JSON.stringify(mutation.mutation.version)) ]])
        }])
        this.locker.commit(mutation)
        return future.promise
    }

    async _rotate (group) {
        for (;;) {
            const group = await this.locker.rotating.promise
            if (group == null) {
                break
            }
            await this._writeahead.rotate()
            for (const [ amalgamator, { key, options } ]  of this._amalgamators) {
                const storage = await WriteAheadOnly.open({
                    ...options,
                    writeahead: this._writeahead,
                    key: [ key, group ],
                    create: [ [ 'locate' ], key ]
                })
                amalgamator.rotate({
                    name: `stage.${group}`,
                    group: group,
                    count: 0,
                    key: group,
                    storage: storage,
                    appending: true
                })
            }
            this.locker.rotated()
            await this.locker.amalgamating.promise
            const mutator = { completed: this._completed  }
            for (const amalgamator of this._amalgamators.keys()) {
                await amalgamator.amalgamate(mutator)
            }
            this.locker.amalgamated()
            await this.locker.unstaging.promise
            for (const amalgamator of this._amalgamators.keys()) {
                this.destructible.progress()
                await amalgamator.unstage()
            }
            while (this._writeahead._logs.length != 1) {
                await this._writeahead.shift()
            }
            this.locker.unstaged()
        }
    }

    _drain () {
        return ([ ...this._amalgamators.keys() ])
            .map(amalgamator => amalgamator.drain())
            .filter(drain => drain != null)
    }

    drain () {
        let drains = this._drain()
        if (drains.length != 0) {
            return (async () => {
                do {
                    for (const promise of drains) {
                        await promise
                    }
                    drains = this._drain()
                } while (drains.length != 0)
            }) ()
        }
        return null
    }

    done () {
        return this.destructible.done.promise
    }
}

module.exports = Rotator
