'use strict'

// Node.js API.
const path = require('path')
const fs = require('fs').promises
const assert = require('assert')

let count = 0

// Handle or rethrow exceptions based on exception properties.
const rescue = require('rescue')

// Return the first non-`null`-like value.
const { coalesce } = require('extant')

const { Trampoline } = require('reciprocate')

// Sort function generator.
const ascension = require('ascension')
// Extract the sorted field from an object.
const whittle = require('whittle')

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
    // Retrieve a set of values from a Strata b-tree.
    skip: require('skip'),
    // Convert iterator results with a conversion function.
    twiddle: require('twiddle'),
    // Splice a versioned staging tree into a primary tree.
    splice: require('splice')
}

// A `async`/`await` durable b-tree.
const Strata = require('b-tree')
const FileSystem = require('b-tree/filesystem')
const WriteAheadOnly = require('b-tree/writeahead')
const Magazine = require('magazine')
const Fracture = require('fracture')

// Reference counts are kept in an array where the reference count is indexed by
// the position of the stage in the stage array at the time the reference was
// taken. We can merge a stage into the primary tree when it is no longer in the
// zero position of the stage array and the reference count in the zero position
// of the reference array reaches zero. Obviously, the reference count in the
// zero position of the reference array will not increase from zero after the
// has been shifted out of the zero position of the stage array.

// The reference count is not based on readers or writers, but the position.
// Writers do not want the zero position stage to be merged because they are
// still writing to it.

// Readers do not want the zero position stage to be merged because they may be
// reading only a subset of the versions for that stage. When the stage
// is merged the version will be changed to zero and unless they stage has a
// version that overrides the zero, the new value will be returned from a
// reader, ruining the isolation.

// We need three stages, rotate, amalgamate and unstage so we can go from the
// kick-off to waiting for writers to drain to waiting for readers to drain.

// We no longer assert that there are only two stages. We instead amalgamate a
// homogenized set of stages, all the stages after the rotated stage, so that
// once that succeeds we can eliminate all of the old logs and old stages.

//
class Amalgamator {
    static Error = Interrupt.create('Amalgamator.Error', {
        DOES_NOT_EXIST: 'database does not exist',
        NOT_A_DATABASE: 'database directory does not contain a database',
        ALREADY_EXISTS: 'attempted to create a database where one already exists',
        NOT_SAME_STAGE: 'destructible and destructible of turnstile must be in the same shutdown stage'
    })

    constructor (destructible, rotator, open, options) {
        // Implement the Destructible deferrable pattern.
        this.destructible = destructible

        this.rotator = rotator

        // Whether or not this Amalgamator should check for conflicts.
        this._conflictable = coalesce(options.conflictable, true)

        // For staging we wrap the application key comparator in a comparator
        // that will include the version and order of the operation. The order
        // is the order in which we processed the record in a batch.
        const stage = ascension([ options.comparator, Number, -1, Number, -1 ], true)

        this.comparator = {
            primary: options.comparator,
            stage: {
                key: stage,
                item: whittle(stage, item => item.key, true)
            }
        }

        // Transforms an application operation into an `Amalgamator` operation.
        this._transformer = options.transformer

        this._turnstile = options.turnstile

        // The Strata b-tree cache to use to store pages.
        this._pages = options.pages

        this._stages = []

        const strata = { stage: null, primary: null }

        // Number of records in a staging tree after which the tree is merged
        // into the primary tree.
        strata.stage = coalesce(strata.stage, {})

        // Primary and staging tree split, merge and vacuum properties.
        strata.primary = coalesce(strata.primary, {})

        strata.stage.leaf = coalesce(strata.stage.leaf, {})
        strata.stage.leaf.split = coalesce(strata.stage.leaf.split, 4096)
        strata.stage.leaf.merge = coalesce(strata.stage.leaf.split, 1024)
        strata.stage.branch = coalesce(strata.stage.branch, {})
        strata.stage.branch.split = coalesce(strata.stage.branch.split, 4096)
        strata.stage.branch.merge = coalesce(strata.stage.branch.split, 1024)

        strata.primary.leaf = coalesce(strata.primary.leaf, {})
        strata.primary.leaf.split = coalesce(strata.primary.leaf.split, 4096)
        strata.primary.leaf.merge = coalesce(strata.primary.leaf.split, 1024)
        strata.primary.branch = coalesce(strata.primary.branch, {})
        strata.primary.branch.split = coalesce(strata.primary.branch.split, 4096)
        strata.primary.branch.merge = coalesce(strata.primary.branch.split, 1024)

        this.strata = strata

        {
            const destructible = this.destructible.durable($ => $(), 'primary')
            const storage = new FileSystem.Writer(destructible.durable($ => $(), 'storage'), open.storage)
            this.primary = new Strata(destructible.durable($ => $(), 'strata'), {
                ...options.primary,
                storage: storage,
                turnstile: options.turnstile,
                pages: options.pages.subordinate(),
                comparator: this.comparator.primary,
                extractor: options.extractor
            })
            this.primary.deferrable.increment()
        }

        for (const stage of open.stages) {
            this._newStage(stage)
            this._stages.push(stage)
        }

        this.destructible.destruct(() => {
            this.destructible.ephemeral('shutdown', async () => {
                for (const strata of [ this.primary ].concat(this._stages.map(stage => stage.strata))) {
                    strata.deferrable.decrement()
                }
                await this.drain()
            })
        })
    }

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

    //

    // Generate a relatively unique filename for a staging file, hopefully we
    // won't be creating new staging files in less than a millisecond.
    _filestamp () {
        return String(Date.now())
    }

    // Create a new staging tree. The caller will determine if the tree should
    // be opened or created.
    _newStage (open) {
        const destructible = open.destructible = this.destructible.ephemeral($ => $(), open.name)
        const storage = new WriteAheadOnly.Writer(destructible.durable($ => $(), 'storage'), open.storage)
        open.strata = new Strata(destructible.durable($ => $(), 'strata'), {
            ...this.strata.stage,
            storage: storage,
            turnstile: this._turnstile,
            comparator: this.comparator.stage.key,
            partition: 1,
            pages: this._pages.subordinate(),
            // Meta information is used for merge and that is the thing we're
            // calling a header. The key's in branches will not need meta
            // information so we'll be able to serialize it without any
            // assistance from the user. I've wanted to serialize as binary, but
            // really I don't see what's wrong with JSON and it makes the files
            // human readable. Revisit with performance testing if you're
            // searching for optimizations.
        })
        open.strata.deferrable.increment()
    }

    map (snapshot, set, {
        extractor = $ => $,
        additional = [],
        group = null
    } = {}) {
        const skip = mvcc.skip.strata(this.primary, set, {
            extractor: extractor,
            group: group ? group : (sought, key, found) => found
        })
        const primary = mvcc.twiddle(skip, items => {
            return items.map(item => {
                item.items = item.items.map(item => {
                    return {
                        key: [ item.key, 0, 0 ],
                        parts: [{ method: 'insert', version: 0, order: 0 }].concat(item.parts)
                    }
                })
                return item
            })
        })
        const skips = this._stages.filter(stage => {
            return snapshot.groups.some(group => group.group == stage.group)
        }).map(stage => {
            const skip = mvcc.skip.strata(stage.strata, set, {
                extractor: $ => [ extractor($) ],
                group: group ? (sought, key) => {
                    return group(sought[0], key[0], false)
                } : (sought, key) => {
                    return this.comparator.primary(sought[0], key[0]) == 0
                }
            })
            return mvcc.twiddle(skip, items => {
                return items.map(item => {
                    item.key = item.key[0]
                    return item
                })
            })
        }).concat(primary).concat(additional.map(array => {
            const skip = mvcc.skip.array(this.comparator.stage.key, array, set, {
                extractor: $ => [ extractor($) ],
                group: (sought, items, index) => {
                    const key = items[index].key
                    return this.comparator.stage.key([ sought[0], key[1], key[2] ], key) == 0
                }
            })
            return mvcc.twiddle(skip, items => {
                return items.map(item => {
                    item.key = item.key[0]
                    return item
                })
            })
        }))
        const homogenized = mvcc.homogenize(this.comparator.stage.key, skips)
        const diluted = mvcc.twiddle(homogenized, items => {
            return items.map(item => {
                item.items = item.items.filter(item => this.rotator.locker.visible(item.key[1], snapshot))
                return item
            })
        })
        return mvcc.designate(this.comparator.primary, diluted)
    }

    iterator (snapshot, direction, key, inclusive, additional = []) {
        // If we are exclusive we will use a maximum version going forward and a
        // minimum version going backward, puts us where we'd expect to be if we
        // where doing exclusive with the external key only.
        //
        // TODO Not sure what no key plus exclusive means.
        //
        // The key alone puts us at either the newest version of the sought key
        // or the newest version of the next record going forward or the oldest
        // version of the previous record going backward.
        //
        // The key plus a zero version puts either at the oldest version of the
        // sought record or the oldest version of the previous record going
        // backward or the newest version of the next record going forward.
        //
        // Whether the actual `riffle` search of a is inclusive or exclusive is
        // immaterial since the search is always by a partial key. It does apply
        // for a search of the primary tree.
        const reverse = direction == 'reverse'

        const keys = key == null
            ? direction == 'reverse'
                ? { staged: Strata.MAX, primary: Strata.MAX }
                : { staged: Strata.MIN, primary: Strata.MIN }
            : { staged: [ key ], primary: key }

        const riffle = mvcc.riffle(this.primary, keys.primary, { slice: 32, inclusive, reverse })

        const primary = mvcc.twiddle(riffle, items => {
            return items.map(item => {
                return {
                    key: [ item.key, 0, 0 ],
                    parts: [{
                        method: 'insert', version: 0, order: 0
                    }].concat(item.parts)
                }
            })
        })

        // Horrors. Either we find a way to nest paritial comparisons, or else
        // provide a means to trim the key other than prividing a comparator,
        // either a nested structure which is terrible, or a rewrite function,
        // or else we flatten our versioned key so that our version information
        // is added to the end of the key array. At this point, I'd say adding
        // the trim function would get this done in a matter of minutes, whereas
        // flattening would take a lot for figuring.
        const riffles = this._stages.filter(stage => {
            return snapshot.groups.some(group => group.group == stage.group)
        }).map(stage => {
            return mvcc.riffle(stage.strata, keys.staged, { slice: 32, reverse, inclusive })
        }).concat(primary).concat(additional)
        const homogenize = mvcc.homogenize(this.comparator.stage.key, riffles)
        const visible = mvcc.dilute(homogenize, item => {
            return this.rotator.locker.visible(item.key[1], snapshot) ? 1 : 0
        })
        const designate = mvcc.designate(this.comparator.primary, visible)
        return mvcc.dilute(designate, item => item.parts[0].method == 'remove' ? 0 : 1)
    }

    get (snapshot, trampoline, key, consume) {
        const candidates = [], stages = this._stages.filter(stage => {
            return snapshot.groups.some(group => group.group == stage.group)
        })
        const get = () => {
            if (stages.length == 0) {
                const winner = coalesce(candidates.sort(this.comparator.stage.item)[0], {
                    parts: [{ method: 'remove' }]
                })
                consume(winner.parts[0].method == 'remove' ? null : winner)
            } else {
                stages.shift().strata.search(trampoline, [ key ], cursor => {
                    let { index, page: { items } } = cursor
                    while (
                        index < items.length &&
                        this.comparator.primary(items[index].key[0], key) == 0
                    ) {
                        if (this.rotator.locker.visible(items[index].key[1], snapshot)) {
                            candidates.push(items[index])
                            break
                        }
                        index++
                    }
                    get()
                })
            }
        }
        this.primary.search(trampoline, key, cursor => {
            const { index, found, page: { items } } = cursor
            if (cursor.found) {
                candidates.push({
                    key: [ items[index].key, 0, 0 ],
                    parts: [{
                        method: 'insert', version: 0, order: 0
                    }].concat(items[index].parts)
                })
            }
            get()
        })
    }
    //

    // When our writing stage has no writes, don't rotate it, just push the
    // group onto its array of group ids. Otherwise, create a new stage and
    // unshift it onto our list of stages.

    // **TODO** This will have to change if we start to use a write-ahead log
    // because we are going to rotate the log and destroy previous log entries.
    // Fortunately, create and destroy of write-ahead log strata is cheap.

    //
    rotate (stage) {
        this._newStage(stage)
        this._stages.unshift(stage)
    }

    // **TODO** Mark amlamgamated and filter out amalgamated on read.
    // **TODO** Merge all stages using homogenize and go all at once.
    // **TODO** Why is that only completed mutator correct?
    async _amalgamate (stack, mutator, stage) {
        assert(stack instanceof Fracture.Stack)
        const riffle = mvcc.riffle(stage.strata, Strata.MIN)
        const visible = mvcc.dilute(riffle, item => {
            return this.rotator.locker.visible(item.key[1], mutator) ? 1 : 0
        })
        const designate = mvcc.designate(this.comparator.primary, visible)
        await mvcc.splice(stack, item => {
            this.destructible.progress()
            return {
                key: item.key[0],
                parts: item.parts[0].method == 'insert' ? item.parts.slice(1) : null
            }
        }, this.primary, designate)
    }

    // We amalgamate all stages except for the first. During normal operation we
    // will only have two stages in the stages array. We may have more than two
    // during a recovery.

    //
    async amalgamate (stack, mutator) {
        assert(stack instanceof Fracture.Stack)
        for (const stage of this._stages.slice(1)) {
            await this._amalgamate(stack, mutator, stage)
        }
    }

    // Unstage removes the amalgamated stages from the end of stage array. We
    // may have had an empty first stage during rotate that we indended to
    // continue to use by unshifting the new group onto its array of group ids,
    // so we pop the old group id. Otherwise we created a new stage. We have
    // more one old stage during recovery. It is also likely, during recovery,
    // that we both reused the first stage and have multiple old stages.

    //
    async unstage () {
        while (this._stages.length != 1) {
            const stage = this._stages.pop()
            stage.destructible.destroy()
            stage.strata.deferrable.decrement()
            await stage.strata.deferrable.done
        }
    }
    //

    // Gather the versions of competitors.

    //
    _conflicted (mutator, items, index, key) {
        for (
            let i = index, I = items.length;
            i < I && this.comparator.primary(items[i].key[0], key) == 0;
            i++
        ) {
            mutator.mutation.competitors.add(items[i].key[1])
            //this.rotator.locker.conflicted(items[i].key[1], mutator)
        }
        for (
            let i = index - 1;
            i >= 0 && this.comparator.primary(items[i].key[0], key) == 0;
            i--
        ) {
            mutator.mutation.competitors.add(items[i].key[1])
            //this.rotator.locker.conflicted(items[i].key[1], mutator)
        }
    }
    //

    // Amalgamate implements opportunistic locking. A mutation will proceed
    // without blocking at any point due to concurrent mutations. The
    // application will check a conflicted flag at the end of the mutation. If
    // the mutation is conflicted, the application will roll back the
    // mutation and then repeat its procedure with a new mutation. It will
    // repeat this process until the procedure is completes with the mutation
    // unconflicted or until it gives up.

    // Here I attempt to convince myself that conflicts will always be detected
    // and that application progress is possible.

    // When a mutation commits it checks for conflicts with competing mutations
    // which we will call competitors.

    // The version number in a record is the version number of the mutation that
    // inserted the record.

    // Deletes are insertions of tombstone records so all operations are
    // inserts.

    // Inserts include a version number. The version number is the version
    // number of the mutation that performed the insert.

    // Mutations insert their records regardless of conflict because of
    // isolation. The application will only become aware of conflict when and if
    // it commits the mutation. The application will expect its inserts to be
    // visible until it commits or rolls back the mutation.

    // At any point after insert and before commit the mutation will gather all
    // the versions for all the records it inserted. That is, it will gather all
    // the version numbers for all the other records in the staging trees that
    // have the same application key.

    // Note that, we perform this gathering as we go along. There are only ever
    // at most two staging trees and only during rotation, otherwise there is
    // just one. Because we group all records for a given key on a single b-tree
    // leaf page we can gather all the versions for a given key in a given
    // staging tree with a single descent of the tree. We can perform this
    // gathering for our active staging in the same descent we use to perform
    // the insert. We search the secondary staging tree similarly looking to
    // optimize our descent of tree.

    // Note too that the set of all competitor versions can be no bigger than
    // the set of all un-amalgamated versions which we are already keeping in
    // memory. If this sounds like a lot of caching, it is not.

    // As noted, we can obtain the set of competitor versions for each key at
    // any point after we write and before we commit. It does not matter if
    // inserts arrive after we gather competitors and before we commit.

    // The conflict check performed prior to commit is synchronous and atomic.

    // If a mutation determines that it conflicts with _any_ competitor it rolls
    // _itself_ back. A mutation conflicts with a competitor if that competitor
    // was created after the mutation was created and if the competitor has not
    // rolled back.

    // Therefore, the first to _detect_ conflict loses.

    // If two mutations conflict, one of them will perform conflict
    // determination.

    // An insert is atomic.

    // After an insert a mutation gathers all versions inserted including
    // competing versions.

    // If there is an existing competing version its mutation will be added to
    // the set of mutations that will be checked for conflicts.

    // If there is an insert after out mutations gathers competitors, that
    // competitor will add our mutation to its set of competitors and perform
    // conflict resolution.

    // If you like you can draw a square and when you do, draw the sides out
    // beyond the corners, and then imagine them starting at the same time, then
    // crossing at different times, see which lines roll themselves back.

    // Only the first two stages are writable and therefore only the first two
    // stages are candidates for conflicts.

    // Given any number of multiple overlapping concurrent transactions, one of
    // them will successfully commit.

    // Spinlock is possible and indeed likely for a transaction that updates a
    // great many records when there are frequent small transactions that also
    // update a subset records it updates. One of the small transactions will
    // cause the large, promiscuous transaction to roll back.

    // It is possible to perform multiple conflict checks as you go, clearing
    // out the list of competitors as you do, so long as you resume gathering
    // after performing the check. Although, you will not need to perform
    // subsequent checks after one has marked your mutation as conflicted.

    //
    async merge (stack, mutator, operations) {
        assert(stack instanceof Fracture.Stack)
        const promises = new Set
        const version = mutator.mutation.version
        const group = this.rotator.locker.group(version)
        const unamalgamated = this._stages.filter(stage => stage.appending)
        assert(unamalgamated.length == 1 || unamalgamated.length == 2)
        const stage = unamalgamated[0].group == group ? unamalgamated.shift() : unamalgamated.pop()
        const secondary = unamalgamated.pop()
        const transforms = operations.map(operation => {
            const order = mutator.mutation.order++
            const transform = this._transformer(operation, order)
            return {
                compound: [ transform.key, version, order ],
                order: order,
                ...transform
            }
        })
        let heft = 0
        const conflictable = []
        const trampoline = new Trampoline
        while (transforms.length != 0) {
            stage.strata.search(trampoline, transforms[0].compound, cursor => {
                const { index, found } = cursor
                const insert = ({
                    index, found
                }, {
                    key, parts, method, order, compound
                }) => {
                    if (this._conflictable) {
                        // Making a point of not landing on the first record for
                        // scanning forwards and back unit test coverage.
                        if (secondary != null) {
                            conflictable.push(key)
                        }
                        this._conflicted(mutator, cursor.page.items, index, key)
                    }
                    // TODO The `version` and `order` are already in the key.
                    const header = { version, method, order }
                    if (method == 'insert') {
                         promises.add(cursor.insert(stack, index, compound, [ header ].concat(parts)))
                    } else {
                         promises.add(cursor.insert(stack, index, compound, [ header, key ]))
                    }
                    assert(cursor.page.items[index].heft)
                    heft += cursor.page.items[index].heft
                }
                insert(cursor, transforms.shift())
                stage.count++
                while (transforms.length != 0) {
                    const { index, found } = cursor.indexOf(transforms[0].compound)
                    if (index == null) {
                        break
                    }
                    insert({ index, found }, transforms.shift())
                    stage.count++
                }
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
        }
        conflictable.sort(this.comparator.primary)
        while (conflictable.length != 0) {
            const zeroed = [ conflictable[0], 0, 0 ]
            secondary.strata.search(trampoline, zeroed, cursor => {
                this._conflicted(mutator, cursor.page.items, cursor.index, conflictable[0])
                conflictable.shift()
                while (conflictable.length != 0) {
                    const zeroed = [ conflictable[0], 0, 0 ]
                    const { index, found } = cursor.indexOf(zeroed)
                    if (index == null) {
                        break
                    }
                    this._conflicted(mutator, cursor.page.items, index, conflictable[0])
                    conflictable.shift()
                }
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
        }
        for (const promise of promises) {
            await promise
        }
        this.rotator.advance()
    }

    _drain () {
        return [
            this.primary.drain()
        ].concat(this._stages.map(stage => stage.strata.drain()))
         .filter(drain => drain != null)
    }

    // **TODO** Need two words `drain` and `full`. No, wait.
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

    get status () {
        const stages = []
        for (const stage of this._stages) {
            const { groups, count, path } = stage
            stages.push({ groups, count, path })
        }
        // TODO Impelement `Destructibe.waiting`.
        return { waiting: this.destructible._waiting.slice(), stages }
    }
}

module.exports = Amalgamator
