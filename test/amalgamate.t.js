require('proof')(30, async okay => {
    function dump (object) {
        console.log(require('util').inspect(object, { depth: null }))
    }

    const assert = require('assert')

    const path = require('path')
    const fs = require('fs').promises

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

    function createAmalgamator (options) {
        const destructible = new Destructible(10000, 'amalgamate.t')
        return new Amalgamator(destructible, {
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
            transformer: function (operation, order) {
                if (operation.type == 'put') {
                    return {
                        method: 'insert',
                        order: order,
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
            await createAmalgamator({ directory: __dirname }).destructible.rejected
        } catch (error) {
            rescue(error, [ Amalgamator.Error, /not a Locket/ ])
            okay('not an appropriate directory')
        }
    }

    {
        try {
            await createAmalgamator({ createIfMissing: false }).destructible.rejected
        } catch (error) {
            rescue(error, [ Amalgamator.Error, /does not exist/ ])
            okay('does not exist')
        }
    }

    {
        const amalgamator = createAmalgamator()

        await amalgamator.ready

        await Destructible.rescue(async function () {
            const snapshots = [ amalgamator.locker.snapshot() ]
            let iterator = amalgamator.iterator(snapshots[0], 'forward', null, true)

            const promises = []
            iterator.next(promises, items => {})
            while (promises.length != 0) {
                await promises.shift()
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
                iterator.next(promises, items => {
                    for (const item of items) {
                        gather.push(item.parts[1].toString(), item.parts[2].toString())
                    }
                })
                while (promises.length != 0) {
                    await promises.shift()
                }
            }
            okay(gather, [ 'a', 'A', 'c', 'C' ], 'forward iterator')

            gather.length = 0
            iterator = amalgamator.iterator(snapshots[0], 'forward', Buffer.from('a'), false)
            while (! iterator.done) {
                iterator.next(promises, items => {
                    for (const item of items) {
                        gather.push(item.parts[1].toString(), item.parts[2].toString())
                    }
                })
                while (promises.length != 0) {
                    await promises.shift()
                }
            }
            okay(gather, [ 'c', 'C' ], 'forward iterator not inclusive')

            gather.length = 0
            iterator = amalgamator.iterator(snapshots[0], 'reverse', null, true)
            while (! iterator.done) {
                iterator.next(promises, items => {
                    for (const item of items) {
                        gather.push(item.parts[1].toString(), item.parts[2].toString())
                    }
                })
                while (promises.length != 0) {
                    await promises.shift()
                }
            }
            okay(gather, [ 'c', 'C', 'a', 'A' ], 'reverse iterator max')

            gather.length = 0
            iterator = amalgamator.iterator(snapshots[0], 'reverse', Buffer.from('c'), true)
            while (! iterator.done) {
                iterator.next(promises, items => {
                    for (const item of items) {
                        gather.push(item.parts[1].toString(), item.parts[2].toString())
                    }
                })
                while (promises.length != 0) {
                    await promises.shift()
                }
            }
            okay(gather, [ 'c', 'C', 'a', 'A' ], 'reverse iterator inclusive')

            gather.length = 0
            iterator = amalgamator.iterator(snapshots[0], 'reverse', Buffer.from('c'), false)
            while (! iterator.done) {
                iterator.next(promises, items => {
                    for (const item of items) {
                        gather.push(item.parts[1].toString(), item.parts[2].toString())
                    }
                })
                while (promises.length != 0) {
                    await promises.shift()
                }
            }
            okay(gather, [ 'a', 'A' ], 'reverse iterator exclusive')

            gather.length = 0
            amalgamator.get(snapshots[0], promises, Buffer.from('a'), item => {
                gather.push(item.parts[1].toString(), item.parts[2].toString())
            })
            while (promises.length != 0) {
                await promises.shift()
            }
            okay(gather, [ 'a', 'A' ], 'staged get')

            gather.length = 0
            amalgamator.get(snapshots[0], promises, Buffer.from('b'), item => {
                gather.push(item)
            })
            while (promises.length != 0) {
                await promises.shift()
            }
            okay(gather, [ null ], 'staged get removed')

            gather.length = 0
            amalgamator.get(snapshots[0], promises, Buffer.from('z'), item => {
                gather.push(item)
            })
            while (promises.length != 0) {
                await promises.shift()
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
                iterator.next(promises, items => {
                    for (const item of items) {
                        gather.push(item.parts[1].toString(), item.parts[2].toString())
                    }
                })
                while (promises.length != 0) {
                    await promises.shift()
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
            await createAmalgamator({ errorIfExists: true }).destructible.rejected
        } catch (error) {
            rescue(error, [ Amalgamator.Error, /already exists/ ])
            okay('error if exists')
        }
    }

    {
        const amalgamator = createAmalgamator()

        await Destructible.rescue(async function () {
            const gather = []

            await amalgamator.ready

            await amalgamator.count()

            await amalgamator.locker.rotate()

            okay(amalgamator.status.stages[0].groups, [ 2 ], 'reopen')

            await amalgamator.locker.rotate()

            okay(amalgamator.status.stages[0].groups, [ 3 ], 'no-op rotate')

            const snapshots = [ amalgamator.locker.snapshot() ]

            const promises = []
            let iterator = amalgamator.iterator(snapshots[0], 'forward', null, true)

            while (! iterator.done) {
                iterator.next(promises, items => {
                    for (const item of items) {
                        gather.push(item.parts[1].toString(), item.parts[2].toString())
                    }
                })
                while (promises.length != 0) {
                    await promises.shift()
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
            amalgamator.get(snapshots[0], promises, Buffer.from('n'), item => {
                gather.push(item.parts[1].toString(), item.parts[2].toString())
            })
            while (promises.length != 0) {
                await promises.shift()
            }
            okay(gather, [ 'n', 'N' ], 'amalgamated get')

            gather.length = 0
            amalgamator.get(snapshots[0], promises, Buffer.from('a'), item => {
                gather.push(item)
            })
            while (promises.length != 0) {
                await promises.shift()
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
                iterator.next(promises, items => {
                    for (const item of items) {
                        gather.push(item.parts[1].toString(), item.parts[2].toString())
                    }
                })
                while (promises.length != 0) {
                    await promises.shift()
                }
            }

            okay(gather, [ 'x', 'X', 'y', 'Y', 'z', 'Z' ], 'staged')

            console.log(require('util').inspect(amalgamator.status, { depth: null }))
        })

        await amalgamator.destructible.destroy().rejected
    }

    {
        const amalgamator = createAmalgamator({
            stage: {
                leaf: { split: 16, merge: 16 },
                branch: { split: 16, merge: 16 }
            }
        })

        await Destructible.rescue(async function () {
            const gather = []

            await amalgamator.ready

            await amalgamator.count()

            okay(amalgamator.status.stages.slice().pop().count != 0, 'unclean shutdown')

            const path = amalgamator.status.stages[0].path

            await amalgamator.locker.rotate()

            okay(amalgamator.status.stages[0].path, path, 'reused empty first stage')

            const snapshots = [ amalgamator.locker.snapshot() ]

            const promises = []
            const iterator = amalgamator.iterator(snapshots[0], 'forward', null, true)
            while (! iterator.done) {
                iterator.next(promises, items => {
                    for (const item of items) {
                        gather.push(item.parts[1].toString(), item.parts[2].toString())
                    }
                })
                while (promises.length != 0) {
                    await promises.shift()
                }
            }

            okay(gather, [ 'x', 'X', 'y', 'Y', 'z', 'Z' ], 'staged')

            gather.length = 0
            amalgamator.get(snapshots[0], promises, Buffer.from('x'), item => {
                gather.push(item.parts[1].toString(), item.parts[2].toString())
            })
            while (promises.length != 0) {
                await promises.shift()
            }
            okay(gather, [ 'x', 'X' ], 'staged get')

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
        const amalgamator = createAmalgamator({ conflictable: false })

        await Destructible.rescue(async function () {
            const gather = []

            await amalgamator.ready

            const versions = new Set
            versions.add(6)

            console.log(amalgamator.status.stages.slice())

            await amalgamator.recover(versions)

            okay(amalgamator.status.stages.slice().pop().count != 0, 'unclean shutdown mapped')

            const path = amalgamator.status.stages[0].path

            await amalgamator.locker.rotate()

            okay(amalgamator.status.stages[0].path, path, 'reused empty first stage mapped')

            const promises = []
            const snapshots = [ amalgamator.locker.snapshot() ]

            let iterator = amalgamator.iterator(snapshots[0], 'forward', null, true)
            while (! iterator.done) {
                iterator.next(promises, items => {
                    for (const item of items) {
                        gather.push(item.parts[1].toString(), item.parts[2].toString())
                    }
                })
                while (promises.length != 0) {
                    await promises.shift()
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
                iterator.next(promises, items => {
                    for (const item of items) {
                        gather.push(item.parts[1].toString(), item.parts[2].toString())
                    }
                })
                while (promises.length != 0) {
                    await promises.shift()
                }
            }
            amalgamator.locker.release(snapshots.shift())
            amalgamator.locker.release(snapshots.shift())

            okay(gather, [ 'a', 'A', 'b', 'B', 'c', 'C' ], 'recover rollback')

            await rotate

            console.log('done')
        })

        await amalgamator.destructible.destroy().rejected
    }
})
