// Node.js API.
const path = require('path')
const fs = require('fs').promises
const assert = require('assert')

// Handle or rethrow exceptions based on exception properties.
const rescue = require('rescue')

// Return the first non-`null`-like value.
const coalesce = require('extant')

// Sort function generator.
const ascension = require('ascension')

const Interrupt = require('interrupt')

// Strata b-tree iteration ultilities.
const mvcc = {
    // Filter for the latest version of an MVCC record.
    designate: require('designate'),
    // Filter iterator results with a filter function.
    dilute: require('dilute'),
    // Merge two or more iterators.
    homogenize: require('homogenize'),
    // Iterate through a Strata b-tree.
    riffle: require('riffle'),
    // Convert iterator results with a conversion function.
    twiddle: require('twiddle'),
    // Splice a versioned staging tree into a primary tree.
    splice: require('splice')
}

// A `async`/`await` durable b-tree.
const Strata = require('b-tree')

// Reference counts are kept in an array where the reference count is indexed by
// the position of the stage in the stage array at the time the reference was
// taken. We can merge a stage into the primary tree when it is no longer in the
// zero position of the stage array and the reference count in the zero position
// of the reference array reaches zero. Obviously, the reference count in the
// zero position of the reference array will not increase from zero after the
// has been shifted out of the zero position of the stage array. The reference
// count is not based on readers or writers, but the position. Writers do not
// want the zero position to stage to be merged because they are still writing
// to it. Readers do not want the zero position stage to be merged because they
// may be reading only a subset of the versions for that stage. When the stage
// is merged the version will be changed to zero and unless they stage has a
// version that overrides the zero, the new value will be returned from a
// reader, ruining the isolation.

//
class Amalgamator {
    static Error = Interrupt.create('Amalgamator.Error')

    // Options.
    //
    //  * `transformer` — transform an application operation into an
    //  `Amalgamator` operation with a `method` of `"insert"` or `"remove"`, a
    //  `key` and a `value`.
    //  * `header.compose` — compose a staging tree header record for each
    //  versioned record to accommodate a version counting strategy. TODO More
    //  Docco.
    //  * `header.serialize` — serialize the staging tree header record.
    //  * `header.deserialize` — deserialize the staging tree header record.
    //  * `stage.max` — max size of a staging tree before it is spliced into the
    //  primary tree.
    //  * `stage.leaf.split` — staging tree leaf page record count greater than
    //  which will cause a leaf page to split.
    //  * `stage.leaf.merge` — staging tree leaf page record count less than
    //  which will cause a leaf page to merge with a neighbor.
    //  * `stage.leaf.split` — staging tree branch page record count greater
    //  than which will cause a branch page to split.
    //  * `stage.leaf.merge` — staging tree leaf page record count less than
    //  which will cause a leaf page to merge with a neighbor.
    //  * `primary.leaf.split` — primary tree leaf page record count greater
    //  than which will cause a leaf page to split.
    //  * `primary.leaf.merge` — primary tree leaf page record count less than
    //  which will cause a leaf page to merge with a neighbor.
    //  * `primary.leaf.split` — primary tree branch page record count greater
    //  than which will cause a branch page to split.
    //  * `primary.leaf.merge` — primary tree leaf page record count less than
    //  which will cause a leaf page to merge with a neighbor.

    //
    constructor (destructible, options) {
        this.destructible = destructible
        // Directory in which the data for this index is stored.
        this.directory = options.directory
        // For staging we wrap the application key comparator in a comparator
        // that will include the version and order of the operation. The order
        // is the order in which we processed the record in a batch.
        //
        // TODO If we handle multple batches with the same version, someone
        // needs to maintain the index externally.
        this._comparator = {
            primary: options.key.compare,
            stage: ascension([ options.key.compare, Number, Number ], function (object) {
                return [ object.value, object.version, object.index ]
            })
        }
        // Transforms an application operation into an `Amalgamator` operation.
        this._transformer = options.transformer
        // TODO Need to sort out how we manage destruction, since where we're
        // able to open and close, and we have an open and close method. Do we
        // want to use destructible constructs?
        this._destructible = {
            amalgamate: destructible.durable('amalgamate'),
            unstage: destructible.durable('amalgamate'),
            strata: destructible.durable('strata')
        }
        this._destructible.strata.increment()
        // Ensure only one rotate at a time.
        this._rotating = false
        // The Strata b-tree cache to use to store pages.
        this._cache = options.cache
        // The primary tree.
        this.strata = null
        // The staging trees.
        this._stages = []
        // Header serialization.
        this._header = options.header
        // True when opening has completed.
        this.ready = new Promise(resolve => this._ready = resolve)
        // Extract a key from the record.
        this.extractor = options.key.extractor
        // Number of records in a staging tree after which the tree is merged
        // into the primary tree.
        const stage = coalesce(options.stage, {})
        this._maxStageCount = coalesce(stage.max, 1024)
        // Primary and staging tree split, merge and vacuum properties.
        const primary = coalesce(options.primary, {})
        const leaf = { stage: coalesce(stage.leaf, {}), primary: coalesce(primary.leaf, {}) }
        const branch = { stage: coalesce(stage.branch, {}), primary: coalesce(primary.branch, {}) }
        this._parts = options.parts
        this._key = options.key
        this._strata = {
            stage: {
                leaf: {
                    split: coalesce(leaf.stage.split, 4096),
                    merge: coalesce(leaf.stage.merge, 2048)
                },
                branch: {
                    split: coalesce(branch.stage.split, 4096),
                    merge: coalesce(branch.stage.merge, 2048)
                }
            },
            primary: {
                leaf: {
                    split: coalesce(leaf.primary.split, 4096),
                    merge: coalesce(leaf.primary.merge, 2048)
                },
                branch: {
                    split: coalesce(branch.primary.split, 4096),
                    merge: coalesce(branch.primary.merge, 2048)
                }
            }
        }
        destructible.destruct(() => {
            this._open = false
            destructible.ephemeral('shutdown', async () => {
                await this._destructible.amalgamate.destroy().destructed
                this._destructible.strata.decrement()
                await this._destructible.strata.destructed
                this._cache.purge(0)
            })
        })
        this._destructible.amalgamate.ephemeral('open', this._open(options))
    }

    // Generate a relatively unique filename for a staging file, hopefully we
    // won't be creating new staging files in less than a millisecond.
    _filestamp () {
        return String(Date.now())
    }

    // Create a new staging tree. The caller will determine if the tree should
    // be opened or created.
    _newStage (directory) {
        const header = this._header
        const strata = new Strata(this._destructible.strata.ephemeral([ 'stage', directory ]), {
            directory: directory,
            branch: this._strata.stage.branch,
            leaf: this._strata.stage.leaf,
            cache: this._cache,
            // Meta information is used for merge and that is the thing we're
            // calling a header. The key's in branches will not need meta
            // information so we'll be able to serialize it without any
            // assistance from the user. I've wanted to serialize as binary, but
            // really I don't see what's wrong with JSON and it makes the files
            // human readable. Revisit with performance testing if you're
            // searching for optimizations.
            serializer: {
                key: {
                    serialize: ({ value, version, index }) => {
                        const header = { version: version.toString(), index }
                        const buffer = Buffer.from(JSON.stringify(header))
                        return [ buffer ].concat(this._key.serialize(value))
                    },
                    deserialize: (parts) => {
                        const { version, index } = JSON.parse(parts[0].toString())
                        return {
                            value: this._key.deserialize(parts.slice(1)),
                            version: version,
                            index: index
                        }
                    }
                },
                // A memorable moment. Included in the header is the count of
                // operations given to `merge`. This is only useful when `merge`
                // is used once per version. Upon reopening the data store you
                // can scan a stage to see if the count of operations in the
                // header matches the number of operations on disk. If so, the
                // version is valid and can be added to the set of valid
                // versions. This is wasteful, however. The count is repeated in
                // every record, but I assume that given the `put` method of
                // LevelDB, inserting a single record per verison is a common
                // case, in which case the count is not wasteful it all. It is
                // economical to tuck it into the header.
                //
                // But, for Memento and IndexedDB where `merge` will be called
                // multiple times it is not useful at all. Ideally we would make
                // this configurable so that such databases wouldn't pay that
                // price, either with a `count` switch or by having the user
                // determine what should go in the header and how it should be
                // serialized.
                //
                // I don't want to document the former and I don't want to have
                // to type out the latter in each dependent project.
                //
                // And to dispell a brainstorm I've having at the time of
                // writing – you cannot have some sort of cummulative count that
                // is any use to a multi-`merge` database. You cannot record a
                // count of three and when you get a new merge of four items
                // record a count of seven. How do you know of the transition
                // from three to seven if there is a failure before the first
                // record with a count of seven is written? The three items will
                // look valid and a partial write will be committed.
                //
                // And for all that, I found a compromise so that `count` won't
                // be added when the user creates a `Mutator` instead of calling
                // `Amalgamator.merge()`, but I'm sure I'll revisit this and
                // wring my hands over whether the header should be binary.

                //
                parts: {
                    serialize: (parts) => {
                        const header = Buffer.from(JSON.stringify(parts[0]))
                        if (parts[0].method == 'insert') {
                            return [ header ].concat(this._parts.serialize(parts.slice(1)))
                        }
                        return [ header ].concat(this._key.serialize(parts[1]))
                    },
                    deserialize: (parts) => {
                        const header = JSON.parse(parts[0].toString())
                        if (header.method == 'insert') {
                            return [ header, this._parts.deserialize(parts.slice(1)) ]
                        }
                        return [ header ].concat(this._key.deserialize(parts.slice(1)))
                    }
                }
            },
            extractor: function (parts) {
                return {
                    value: this.extractor(parts.slice(1)),
                    version: parts[0].version,
                    index: parts[0].header.index
                }
            },
            comparator: this._comparator.stage
        })
        return {
            strata, path: directory,
            versions: { 0: true },
            appending: true,
            amalgamated: false,
            count: 0,
            references: [ 0, 0 ]
        }
    }

    async _open (options) {
        try {
            const createIfMissing = coalesce(options.createIfMissing, true)
            const errorIfExists = coalesce(options.errorIfExists, false)
            this._open = true
            // TODO Hoist.
            let exists = true
            this._versions = { 0: true }
            // Must be one, version zero must only come out of the primary tree.
            this._version = 1
            const files = await (async () => {
                for (;;) {
                    try {
                        return await fs.readdir(this.directory)
                    } catch (error) {
                        await rescue(error, [{ code: 'ENOENT' }])
                        if (!createIfMissing) {
                            throw new Amalgamator.Error('database does not exist')
                        }
                        await fs.mkdir(this.directory, { recursive: true })
                    }
                }
            }) ()
            const subdirs = [ 'primary', 'staging' ]
            if (exists) {
                const sorted = files.filter(file => file[0] != '.').sort()
                if (!sorted.length) {
                    exists = false
                // TODO Not a very clever recover, something might be in the midst
                // of a rotation.
                } else if (!subdirs.every(file => sorted.shift() == file) || sorted.length) {
                    throw new Amalgamator.Error('not a Locket datastore')
                }
            }
            if (exists && errorIfExists) {
                throw new Amalgamator.Error('database already exists')
            }
            if (!exists) {
                for (const dir of subdirs) {
                    await fs.mkdir(path.join(this.directory, dir), { recursive: true })
                }
            }
            // TODO Either use destructible correctly or bring it internal to
            // Strata.
            this.strata = new Strata(this._destructible.strata.durable('primary'), {
                directory: path.join(this.directory, 'primary'),
                cache: this._cache,
                comparator: this._comparator.primary,
                serializer: 'buffer',
                branch: this._strata.primary.branch,
                leaf: this._strata.primary.leaf,
                extractor: this.extractor
            })
            if ((await fs.readdir(path.join(this.directory, 'primary'))).length != 0) {
                await this.strata.open()
            } else {
                await this.strata.create()
            }
            const staging = path.join(this.directory, 'staging')
            for (const file of await fs.readdir(staging)) {
                const stage = this._newStage(path.join(staging, file)), counts = {}
                await stage.strata.open()
                this._stages.push(stage)
            }
            const directory = path.join(staging, this._filestamp())
            await fs.mkdir(directory, { recursive: true })
            const stage = this._newStage(directory)
            await stage.strata.create()
            this._stages.push(stage)
        } finally {
            this._ready.call()
        }
    }

    iterator (versions, direction, key, inclusive, additional = []) {
        const stages = this._stages.filter(stage => ! stage.amalgamated)

        stages.forEach((stage, index) => stage.references[index]++)
        assert(stages.every(stage => stage.references.every(count => ! isNaN(count))))

        // If we are exclusive we will use a maximum version going forward and a
        // minimum version going backward, puts us where we'd expect to be if we
        // where doing exclusive with the external key only.
        const version = direction == 'forward'
            ? inclusive ? 0 : Number.MAX_SAFE_INTEGER
            : inclusive ? Number.MAX_SAFE_INTEGER : 0
        // TODO Not sure what no key plus exclusive means.
        const versioned = key != null
            ? { value: key, version: version }
            : direction == 'forward'
                ? Strata.MIN
                : Strata.MAX
        const compound = typeof versioned == 'symbol' ? versioned : versioned.value

        const riffle = mvcc.riffle[direction](this.strata, compound, 32, inclusive)
        const primary = mvcc.twiddle(riffle, items => {
            // TODO Looks like I'm in the habit of adding extra stuff, meta stuff,
            // so the records, so go back and ensure that I'm allowing this,
            // forwarding the meta information.
            return items.map(item => {
                return {
                    key: { value: item.parts[0], version: 0, index: 0 },
                    parts: [{
                        header: { method: 'put' },
                        version: 0
                    }, item.parts[0], item.parts[1]]
                }
            })
        })

        const riffles = this._stages.map(stage => {
            return mvcc.riffle[direction](stage.strata, versioned, {
                slice: 32, inclusive: inclusive
            })
        }).concat(primary).concat(additional)
        const homogenize = mvcc.homogenize[direction](this._comparator.stage, riffles)
        const designate = mvcc.designate[direction](this._comparator.primary, homogenize, versions)
        const dilute = mvcc.dilute(designate, item => {
            return item.parts[0].method == 'remove' ? -1 : 0
        })
        const iterator = {
            [Symbol.asyncIterator]: function () {
                return this
            },
            riffles: 0,
            next: async function () {
                const next = await dilute.next()
                if (next.done) {
                    this['return']()
                }
                return next
            },
            'return': function () {
                stages.splice(0).forEach((stage, index) => stage.references[index]--)
            }
        }
        return iterator
    }

    async _unstage () {
        const stage = this._stages.pop()
        await stage.strata.destructible.destroy().destructed
        // TODO Implement Strata.options.directory.
        await fs.rmdir(stage.path, { recursive: true })
        this._maybeUnstage()
    }

    _maybeUnstage () {
        if (this._open && this._stages.length > 1) {
            const stage = this._stages[this._stages.length - 1]
            if (stage.amalgamated && stage.references.reduce((sum, value) => sum + value, 0) == 0) {
                this._destructible.unstage.ephemeral([ 'unstage', stage.path ], this._unstage())
            }
        }
    }

    async _amalgamate () {
        const stage = this._stages[this._stages.length - 1]
        assert.equal(stage.references.reduce((sum, value) => sum + value), 0)
        const riffle = mvcc.riffle.forward(stage.strata, Strata.MIN)[Symbol.asyncIterator]()
        const working = {
            [Symbol.asyncIterator]: function () { return this },
            next: async () => {
                const next = riffle.next()
                this._destructible.amalgamate.working()
                return next
            }
        }
        const designate = mvcc.designate.forward(this._comparator.primary, working, stage.versions)
        await mvcc.splice(item => {
            return {
                key: item.key.value,
                parts: item.parts[0].method == 'insert' ? item.parts.slice(1) : null
            }
        }, this.strata, designate)
        stage.amalgamated = true
        this._maybeUnstage()
    }

    _maybeAmalgamate () {
        if (this._open && this._stages.length > 1) {
            const stage = this._stages[this._stages.length - 1]
            if (!stage.amalgamated && stage.references[0] == 0) {
                this._destructible.amalgamate.ephemeral('amalgamate', async () => {
                    await this._amalgamate()
                    this._maybeAmalgamate()
                })
            }
        }
    }

    async merge (version, operations) {
        const mutator = this.mutator(version)
        await mutator.merge(operations, { count: operations.length })
        mutator.commit()
    }

    mutator (version) {
        return new Mutator(this, version)
    }

    _maybeNewStage () {
        // A race to create the next stage, but the loser will merely create a stage
        // taht will be unused or little used.
        if (
            ! this._rotating &&
            this._stages[0].count > this._maxStageCount &&
            this._stages.length == 1
        ) {
            this._rotating = true
            this._destructible.amalgamate.ephemeral('rotate', async () => {
                const directory = path.join(this.directory, 'staging', this._filestamp())
                await fs.mkdir(directory, { recursive: true })
                const next = this._newStage(directory, {})
                await next.strata.create()
                this._stages.unshift(next)
                this._rotating = false
            })
        }
        this._maybeAmalgamate()
        this._maybeUnstage()
    }
}

class Mutator {
    constructor (amalgamator, version) {
        this._amalgamator = amalgamator
        this._version = version
        this._stage = amalgamator._stages[0]
        this._stage.references[0]++
    }

    async merge (operations, extra = {}) {
        const {
            _amalgamator: { _transformer: transformer },
            _stage: stage,
            _version: version
        } = this, writes = {}
        let cursor = Strata.nullCursor(), found, index = 0, i = 0
        for (let i = 0, I = operations.length; i < I; i++) {
            const { method, key, parts, index: _index } = transformer(operations[i], i)
            const compound = { value: key, version, index: _index }
            for (;;) {
                ; ({ index, found } = cursor.indexOf(compound, cursor.page.ghosts))
                if (index != null) {
                    break
                }
                cursor.release()
                cursor = await stage.strata.search(compound)
            }
            const header = {
                version: version,
                method: method,
                index: _index,
                ...extra
            }
            if (method == 'insert') {
                cursor.insert(index, compound, [ header ].concat(parts), writes)
            } else {
                assert.equal(method, 'remove')
                cursor.insert(index, compound, [ header, key ], writes)
            }
            stage.count++
        }
        cursor.release()
        await Strata.flush(writes)
    }

    commit () {
        this._stage.versions[this._version] = true
        this._release()
    }

    rollback () {
        this._release()
    }

    _release () {
        this._stage.references[0]--
        this._amalgamator._maybeNewStage()
    }
}

module.exports = Amalgamator
