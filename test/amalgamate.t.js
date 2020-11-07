require('proof')(35, async okay => {
    function dump (object) {
        console.log(require('util').inspect(object, { depth: null }))
    }

    const assert = require('assert')

    const path = require('path')
    const fs = require('fs').promises

    const Trampoline = require('reciprocate')
    const Destructible = require('destructible')
    const Amalgamator = require('..')
    const Locker = require('../locker')

    const rescue = require('rescue')

    const Cache = require('b-tree/cache')

    const directory = path.join(__dirname, 'tmp', 'amalgamate')

    await fs.rmdir(directory, { recursive: true })

    const alphabet = 'abcdefghijklmnopqrstuvwxyz'.split('')

    const put = alphabet.map(letter => {
        return {
            type: 'put', key: Buffer.from(letter), value: Buffer.from(letter.toUpperCase())
        }
    })
    const del = alphabet.map(letter => {
        return { type: 'del', key: Buffer.from(letter) }
    })

    const locker = new Locker({ heft: 1024 })
    await locker.rotate()

    // TODO Why did I do this as buffers? Tests would be so much easier as
    // strings.
    function createAmalgamator (options) {
        const destructible = new Destructible(10000, 'amalgamate.t')
        return Amalgamator.open(destructible, {
            directory: directory,
            locker: new Locker({ heft: 1024 * 8 }),
            cache: new Cache,
            comparator: Buffer.compare,
            createIfMissing: true,
            errorIfExists: false,
            key: {
                compare: Buffer.compare,
                extract: function (parts) { return parts[0] },
                serialize: function (key) { return [ key ] },
                deserialize: function (parts) { return parts[0] }
            },
            parts: {
                serialize: function (parts) { return parts },
                deserialize: function (parts) { return parts }
            },
            transformer: function (operation) {
                if (operation.type == 'put') {
                    return {
                        method: 'insert',
                        key: operation.key,
                        parts: [ operation.key, operation.value ]
                    }
                }
                return {
                    method: 'remove',
                    key: operation.key
                }
            },
            primary: {
                leaf: { split: 256, merge: 32 },
                branch: { split: 256, merge: 32 },
            },
            stage: {
                leaf: { split: 256, merge: 32 },
                branch: { split: 256, merge: 32 },
            },
            ...options
        })
    }

    {
        try {
            const amalgamator = await createAmalgamator({ directory: __dirname })
            await amalgamator.destructible.rejected
        } catch (error) {
            rescue(error, [ Amalgamator.Error, { code: 'NOT_A_DATABASE' } ])
            okay('not an appropriate directory')
        }
    }

    {
        try {
            const amalgamator = await createAmalgamator({ createIfMissing: false })
            await amalgamator.destructible.rejected
        } catch (error) {
            rescue(error, [ Amalgamator.Error, { code: 'DOES_NOT_EXIST' } ])
            okay('does not exist')
        }
    }

    {
        const amalgamator = await createAmalgamator()

        await Destructible.rescue(async function () {
            const snapshots = [ amalgamator.locker.snapshot() ]
            let iterator = amalgamator.iterator(snapshots[0], 'forward', null, true)

            const trampoline = new Trampoline
            iterator.next(trampoline, items => {})
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            okay(iterator.done, 'empty')

            amalgamator.locker.release(snapshots.shift())

            const mutator = amalgamator.locker.mutator()

            await amalgamator.merge(mutator, [{
                type: 'put',
                key: Buffer.from('a'),
                value: Buffer.from('A')
            }, {
                type: 'put',
                key: Buffer.from('b'),
                value: Buffer.from('B')
            }, {
                type: 'put',
                key: Buffer.from('c'),
                value: Buffer.from('C')
            }, {
                type: 'del',
                key: Buffer.from('b')
            }], true)

            okay(mutator.conflicted, false, 'no conflicts')

            amalgamator.locker.commit(mutator)

            snapshots.push(amalgamator.locker.snapshot())

            const gather = []
            iterator = amalgamator.iterator(snapshots[0], 'forward', Buffer.from('a'), true)
            while (! iterator.done) {
                iterator.next(trampoline, items => {
                    for (const item of items) {
                        gather.push(item.parts[1].toString(), item.parts[2].toString())
                    }
                })
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
            }
            okay(gather, [ 'a', 'A', 'c', 'C' ], 'forward iterator')

            gather.length = 0
            iterator = amalgamator.iterator(snapshots[0], 'forward', Buffer.from('a'), false)
            while (! iterator.done) {
                iterator.next(trampoline, items => {
                    for (const item of items) {
                        gather.push(item.parts[1].toString(), item.parts[2].toString())
                    }
                })
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
            }
            okay(gather, [ 'c', 'C' ], 'forward iterator not inclusive')

            gather.length = 0
            iterator = amalgamator.iterator(snapshots[0], 'reverse', null, true)
            while (! iterator.done) {
                iterator.next(trampoline, items => {
                    for (const item of items) {
                        gather.push(item.parts[1].toString(), item.parts[2].toString())
                    }
                })
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
            }
            okay(gather, [ 'c', 'C', 'a', 'A' ], 'reverse iterator max')

            gather.length = 0
            iterator = amalgamator.iterator(snapshots[0], 'reverse', Buffer.from('c'), true)
            while (! iterator.done) {
                iterator.next(trampoline, items => {
                    for (const item of items) {
                        gather.push(item.parts[1].toString(), item.parts[2].toString())
                    }
                })
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
            }
            okay(gather, [ 'c', 'C', 'a', 'A' ], 'reverse iterator inclusive')

            gather.length = 0
            iterator = amalgamator.iterator(snapshots[0], 'reverse', Buffer.from('c'), false)
            while (! iterator.done) {
                iterator.next(trampoline, items => {
                    for (const item of items) {
                        gather.push(item.parts[1].toString(), item.parts[2].toString())
                    }
                })
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
            }
            okay(gather, [ 'a', 'A' ], 'reverse iterator exclusive')

            const set = [ 'a', 'b', 'c', 'd', 'e' ]
            iterator = amalgamator.map(snapshots[0], set.map(letter => Buffer.from(letter)), {
                additional: [[{
                    key: [ Buffer.from('e'), Number.MAX_SAFE_INTEGER, 0 ],
                    parts: [{
                        method: 'insert',
                        version: Math.MAX_SAFE_INTEGER,
                        order: 0
                    }, Buffer.from('e'), Buffer.from('E') ]
                }]]
            })

            gather.length = 0
            while (! iterator.done) {
                iterator.next(trampoline, items => {
                    for (const outer of items) {
                        for (const inner of outer.items) {
                            gather.push({
                                key: inner.key[0].toString(),
                                method: inner.parts[0].method
                            })
                        }
                    }
                })
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
            }
            okay(gather, [{
                key: 'a', method: 'insert',
            }, {
                key: 'b', method: 'remove',
            }, {
                key: 'c', method: 'insert',
            }, {
                key: 'e', method: 'insert',
            }], 'staged map')

            iterator = amalgamator.map(snapshots[0], set.map(letter => Buffer.from(letter)), {
                group: (sought, key, found) => Buffer.compare(sought, key) == 0,
                additional: [[{
                    key: [ Buffer.from('e'), Number.MAX_SAFE_INTEGER, 0 ],
                    parts: [{
                        method: 'insert',
                        version: Math.MAX_SAFE_INTEGER,
                        order: 0
                    }, Buffer.from('e'), Buffer.from('E') ]
                }]]
            })

            gather.length = 0
            while (! iterator.done) {
                iterator.next(trampoline, items => {
                    for (const outer of items) {
                        for (const inner of outer.items) {
                            gather.push({
                                key: inner.key[0].toString(),
                                method: inner.parts[0].method,
                                soughtIsBuffer: Buffer.isBuffer(outer.key)
                            })
                        }
                    }
                })
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
            }
            okay(gather, [{
                key: 'a', method: 'insert', soughtIsBuffer: true
            }, {
                key: 'b', method: 'remove', soughtIsBuffer: true
            }, {
                key: 'c', method: 'insert', soughtIsBuffer: true
            }, {
                key: 'e', method: 'insert', soughtIsBuffer: true
            }], 'staged map with custom group')

            gather.length = 0
            amalgamator.get(snapshots[0], trampoline, Buffer.from('a'), item => {
                gather.push(item.parts[1].toString(), item.parts[2].toString())
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            okay(gather, [ 'a', 'A' ], 'staged get')

            gather.length = 0
            amalgamator.get(snapshots[0], trampoline, Buffer.from('b'), item => {
                gather.push(item)
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            okay(gather, [ null ], 'staged get removed')

            gather.length = 0
            amalgamator.get(snapshots[0], trampoline, Buffer.from('z'), item => {
                gather.push(item)
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            okay(gather, [ null ], 'staged get missing')

            amalgamator.locker.release(snapshots.shift())

            for (let i = 0; i < 128; i++) {
                const mutator = amalgamator.locker.mutator()
                const version = i + 1
                const batch = i == 127 ? put.concat(del.slice(0, 13)) : put.concat(del)
                await amalgamator.merge(mutator, batch, true)
                assert(!mutator.conflicted)
                amalgamator.locker.commit(mutator)
            }

            //dump(amalgamator.locker.status)

            await new Promise(resolve => setTimeout(resolve, 2500))

            //dump(amalgamator.locker.status)

            snapshots.push(amalgamator.locker.snapshot())

            gather.length = 0

            iterator = amalgamator.iterator(snapshots[0], 'forward', null, true)
            while (! iterator.done) {
                iterator.next(trampoline, items => {
                    for (const item of items) {
                        gather.push(item.parts[1].toString(), item.parts[2].toString())
                    }
                })
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
            }
            okay(gather, [
                'n', 'N', 'o', 'O', 'p', 'P',
                'q', 'Q', 'r', 'R', 's', 'S',
                't', 'T', 'u', 'U', 'v', 'V',
                'w', 'W', 'x', 'X', 'y', 'Y',
                'z', 'Z'
            ], 'amalgamate many')

            // TODO Reverse iterator.
        })

        await amalgamator.destructible.destroy().rejected
    }

    {
        try {
            const amalgamator = await createAmalgamator({ errorIfExists: true })
            await amalgamator.destructible.rejected
        } catch (error) {
            rescue(error, [ Amalgamator.Error, { code: 'ALREADY_EXISTS' } ])
            okay('error if exists')
        }
    }

    {
        const amalgamator = await createAmalgamator()

        await Destructible.rescue(async function () {
            const gather = []

            await amalgamator.count()

            await amalgamator.locker.rotate()

            okay(amalgamator.status.stages[0].groups, [ 2 ], 'reopen')

            await amalgamator.locker.rotate()

            okay(amalgamator.status.stages[0].groups, [ 3 ], 'no-op rotate')

            const snapshots = [ amalgamator.locker.snapshot() ]

            const trampoline = new Trampoline
            let iterator = amalgamator.iterator(snapshots[0], 'forward', null, true)

            while (! iterator.done) {
                iterator.next(trampoline, items => {
                    for (const item of items) {
                        gather.push(item.parts[1].toString(), item.parts[2].toString())
                    }
                })
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
            }

            okay(gather, [
                'n', 'N', 'o', 'O', 'p', 'P',
                'q', 'Q', 'r', 'R', 's', 'S',
                't', 'T', 'u', 'U', 'v', 'V',
                'w', 'W', 'x', 'X', 'y', 'Y',
                'z', 'Z'
            ], 'amalgamate many reopen')

            gather.length = 0
            amalgamator.get(snapshots[0], trampoline, Buffer.from('n'), item => {
                gather.push(item.parts[1].toString(), item.parts[2].toString())
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            okay(gather, [ 'n', 'N' ], 'amalgamated get')

            gather.length = 0
            amalgamator.get(snapshots[0], trampoline, Buffer.from('a'), item => {
                gather.push(item)
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            okay(gather, [ null ], 'amalgamated get missing')

            const mutator = amalgamator.locker.mutator()

            okay(mutator.mutation.version, 4, 'clean shutdown')

            await amalgamator.merge(mutator, put.concat(del.slice(0, 23)), true)

            amalgamator.locker.commit(mutator)

            amalgamator.locker.release(snapshots.shift())

            snapshots.push(amalgamator.locker.snapshot())

            gather.length = 0

            iterator = amalgamator.iterator(snapshots[0], 'forward', null, true)
            while (! iterator.done) {
                iterator.next(trampoline, items => {
                    for (const item of items) {
                        gather.push(item.parts[1].toString(), item.parts[2].toString())
                    }
                })
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
            }

            okay(gather, [ 'x', 'X', 'y', 'Y', 'z', 'Z' ], 'staged')

            console.log(require('util').inspect(amalgamator.status, { depth: null }))
        })

        await amalgamator.destructible.destroy().rejected
    }

    {
        const amalgamator = await createAmalgamator({
            stage: {
                leaf: { split: 16, merge: 16 },
                branch: { split: 16, merge: 16 }
            }
        })

        await Destructible.rescue(async function () {
            const gather = []

            await amalgamator.count()

            okay(amalgamator.status.stages.slice().pop().count != 0, 'unclean shutdown')

            const path = amalgamator.status.stages[0].path

            await amalgamator.locker.rotate()

            okay(amalgamator.status.stages[0].path, path, 'reused empty first stage')

            const snapshots = [ amalgamator.locker.snapshot() ]

            const trampoline = new Trampoline
            const iterator = amalgamator.iterator(snapshots[0], 'forward', null, true)
            while (! iterator.done) {
                iterator.next(trampoline, items => {
                    for (const item of items) {
                        gather.push(item.parts[1].toString(), item.parts[2].toString())
                    }
                })
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
            }

            okay(gather, [ 'x', 'X', 'y', 'Y', 'z', 'Z' ], 'primary')

            let set = amalgamator.map(snapshots[0], [ 'v', 'w', 'x', 'z' ].map(letter => Buffer.from(letter)), {
                additional: [[{
                    key: [ Buffer.from('v'), Number.MAX_SAFE_INTEGER, 0 ],
                    parts: [{
                        method: 'insert',
                        version: Number.MAX_SAFE_INTEGER,
                        order: 0
                    }, Buffer.from('v'), Buffer.from('V') ]
                }]]
            })

            gather.length = 0
            while (! set.done) {
                set.next(trampoline, items => {
                    for (const outer of items) {
                        for (const inner of outer.items) {
                            gather.push({
                                key: inner.key[0].toString(),
                                method: inner.parts[0].method,
                                soughtIsBuffer: Buffer.isBuffer(outer.key)
                            })
                        }
                    }
                })
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
            }
            okay(gather, [{
                key: 'v', method: 'insert', soughtIsBuffer: true
            }, {
                key: 'x', method: 'insert', soughtIsBuffer: true
            }, {
                key: 'z', method: 'insert', soughtIsBuffer: true
            }], 'map primary')

            set = amalgamator.map(snapshots[0], [ 'v', 'w', 'x', 'z' ].map(letter => Buffer.from(letter)), {
                group: (sought, key) => Buffer.compare(sought, key) == 0,
                additional: [[{
                    key: [ Buffer.from('v'), Number.MAX_SAFE_INTEGER, 0 ],
                    parts: [{
                        method: 'insert',
                        version: Number.MAX_SAFE_INTEGER,
                        order: 0
                    }, Buffer.from('v'), Buffer.from('V') ]
                }]]
            })

            gather.length = 0
            while (! set.done) {
                set.next(trampoline, items => {
                    for (const outer of items) {
                        for (const inner of outer.items) {
                            gather.push({
                                key: inner.key[0].toString(),
                                method: inner.parts[0].method,
                                soughtIsBuffer: Buffer.isBuffer(outer.key)
                            })
                        }
                    }
                })
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
            }
            okay(gather, [{
                key: 'v', method: 'insert', soughtIsBuffer: true
            }, {
                key: 'x', method: 'insert', soughtIsBuffer: true
            }, {
                key: 'z', method: 'insert', soughtIsBuffer: true
            }], 'map primary with custom group')

            gather.length = 0
            amalgamator.get(snapshots[0], trampoline, Buffer.from('x'), item => {
                console.log(item)
                gather.push(item.parts[1].toString(), item.parts[2].toString())
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            okay(gather, [ 'x', 'X' ], 'primary get')

            const mutators = [ amalgamator.locker.mutator() ]
            okay(mutators[0].mutation.version, 6, 'version advanced')
            amalgamator.locker.release(snapshots.shift())
            await amalgamator.merge(mutators[0], put.slice(0, 6).concat(del.slice(3)))
            amalgamator.locker.commit(mutators.shift())

            mutators.push(amalgamator.locker.mutator())
            await amalgamator.merge(mutators[0], del.concat(del.slice(0, 3)))
            amalgamator.locker.rollback(mutators.shift())

            await amalgamator.drain()
        })

        await amalgamator.destructible.destroy().rejected
    }

    {
        const amalgamator = await createAmalgamator({ conflictable: false })

        await Destructible.rescue(async function () {
            const gather = []

            const versions = new Set
            versions.add(6)

            console.log(amalgamator.status.stages.slice())

            await amalgamator.recover(versions)

            okay(amalgamator.status.stages.slice().pop().count != 0, 'unclean shutdown mapped')

            const path = amalgamator.status.stages[0].path

            await amalgamator.locker.rotate()

            okay(amalgamator.status.stages[0].path, path, 'reused empty first stage mapped')

            const trampoline = new Trampoline
            const snapshots = [ amalgamator.locker.snapshot() ]

            let iterator = amalgamator.iterator(snapshots[0], 'forward', null, true)
            while (! iterator.done) {
                iterator.next(trampoline, items => {
                    for (const item of items) {
                        gather.push(item.parts[1].toString(), item.parts[2].toString())
                    }
                })
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
            }

            okay(gather, [ 'a', 'A', 'b', 'B', 'c', 'C' ], 'recover mapped')
            amalgamator.locker.release(snapshots.shift())

            const mutators = [ amalgamator.locker.mutator() ]

            await amalgamator.merge(mutators[0], del.concat(del.slice(0, 3)))

            const rotate = amalgamator.locker.rotate()

            await new Promise(resolve => setTimeout(resolve, 250))

            await amalgamator.merge(mutators[0], del.concat(del.slice(0, 3)))
            snapshots.unshift(amalgamator.locker.snapshot())
            amalgamator.locker.rollback(mutators.shift())
            snapshots.unshift(amalgamator.locker.snapshot())

            gather.length = 0

            iterator = amalgamator.iterator(snapshots[0], 'forward', null, true)
            while (! iterator.done) {
                iterator.next(trampoline, items => {
                    for (const item of items) {
                        gather.push(item.parts[1].toString(), item.parts[2].toString())
                    }
                })
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
            }

            okay(gather, [ 'a', 'A', 'b', 'B', 'c', 'C' ], 'rollback forward')

            iterator = amalgamator.map(snapshots[0], [ 'a', 'd' ].map(letter => Buffer.from(letter)))
            gather.length = 0
            while (! iterator.done) {
                iterator.next(trampoline, items => {
                    for (const outer of items) {
                        for (const inner of outer.items) {
                            gather.push({
                                key: inner.key[0].toString(),
                                method: inner.parts[0].method,
                                soughtIsBuffer: Buffer.isBuffer(outer.key)
                            })
                        }
                    }
                })
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
            }

            okay(gather, [{
                key: 'a', method: 'insert', soughtIsBuffer: true
            }], 'rollback set')

            amalgamator.locker.release(snapshots.shift())
            amalgamator.locker.release(snapshots.shift())

            await rotate

            console.log('done')
        })

        await amalgamator.destructible.destroy().rejected
    }
})
