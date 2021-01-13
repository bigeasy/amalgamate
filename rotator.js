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
    constructor (destructible, { writeahead, size, checksum, locker }) {
        this.locker = locker
        this.destructible = destructible
        this.destructible.destruct(() => this.locker.destroy())
        this._amalgamators = new Map
        this._writeahead = writeahead
        this._zeroed = Future.resolve()
        this._rotated = Future.resolve()
        this._recorder = Recorder.create(checksum)
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

    static async open (writeahead, { create = false, size = 1024 * 1024, checksum = () => '0' } = {}) {
        const player = new Player(checksum)
        const mutations = new Map
        let version = 0
        for await (const buffer of writeahead.get('commit')) {
            for (const entry of player.split(buffer)) {
                const mutation = {
                    version: JSON.parse(entry.parts.shift()),
                    completed: 0,
                    rolledback: false
                }
                mutations.set(version, mutation)
                version = Math.max(version, mutation.version)
            }
        }
        return { writeahead, size, checksum, locker: new Locker({ mutations, version, size, size }) }
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
                name: `stage.${this.locker.latest}`,
                group: this.locker.latest,
                count: 0,
                key: this.locker.latest,
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

    advance () {
        this.locker.advance(this._writeahead.position)
    }

    commit (mutation) {
        const promise = this._writeahead.write([{
            keys: [ 'commit' ],
            buffer: (this._recorder)([[ Buffer.from(JSON.stringify(mutation.mutation.version)) ]])
        }])
        this.locker.commit(mutation)
        return promise
    }

    async _rotate (group) {
        for (;;) {
            console.log(this.locker.rotating.fulfilled)
            const group = await this.locker.rotating.promise
            console.log('will rotate', group)
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
                    storage: storage
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
                console.log('will unstage')
                await amalgamator.unstage()
                console.log('did unstage')
            }
            this.locker.unstaged()
        }
        console.log('did done')
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
