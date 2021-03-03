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
        this.deferrable = destructible.durable($ => $(), { countdown: 1 }, 'deferrable')
        this.deferrable.destruct(() => this.locker.destroy())
        this.destructible.destruct(() => this.deferrable.decrement())
        this._destructible = {
            amalgamators: this.deferrable.durable('amalgamators')
        }
        this._amalgamators = new Map
        this._writeahead = writeahead
        this._zeroed = Future.resolve()
        this._rotated = Future.resolve()
        this._recorder = Recorder.create(() => '0')
        this.deferrable.durable($ => $(), 'rotate', this._rotate())
        writeahead.deferrable.increment()
        this.deferrable.destruct(() => {
            this.deferrable.ephemeral('shutdown', async () => {
                await this._destructible.amalgamators.done
                writeahead.deferrable.decrement()
            })
        })
    }

    static async open (writeahead) {
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

    async open (name, { directory, handles, create, key, serializer, checksum, extractor }, options) {
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
                                // TODO You do want to merge the arrays when you serialize, but do
                                // you want to do so when you deserialize? Can't the users parts be
                                // in array by themselves so you don't have to slice to pass them
                                // around? We had a bug where we where doing this here with key
                                // below which was a bug when the user was using an array key.
                                //
                                // The user will always use array parts, that is the parts
                                // deserializer always returns an array, but the key serializer can
                                // return a scalar.
                                return [ header ].concat(serializer.parts.deserialize(parts.slice(1)))
                            }
                            return [ header, serializer.key.deserialize(parts.slice(1)) ]
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
        const amalgamator = new Amalgamator(this._destructible.amalgamators.durable(name), this, open, options)
        this._amalgamators.set(amalgamator, { key, options: open.options })
        return amalgamator
    }

    advance () {
        this.locker.advance(this._writeahead.position)
    }

    commit (version) {
        return this._writeahead.write([{
            keys: [ 'commit' ],
            buffer: (this._recorder)([[ Buffer.from(JSON.stringify(version)) ]])
        }], true)
    }

    async _rotate (group) {
        for (;;) {
            const group = await this.locker.rotating.promise
            if (group == null || this.deferrable.destroyed) {
                break
            }
            this.deferrable.increment()
            await this._writeahead.rotate().promise
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
            const mutator = { created: this.locker._completed  }
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
                await this._writeahead.shift().promise
            }
            this.locker.unstaged()
            this.deferrable.decrement()
        }
    }

    _drain () {
        return ([ ...this._amalgamators.keys() ])
            .map(amalgamator => amalgamator.drain())
            .filter(drain => drain != null)
    }

    drain () {
        throw new Error('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
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
