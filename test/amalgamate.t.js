require('proof')(10, async okay => {
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
            locker: new Locker({ heft: 1024 }),
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
                    order: order,
                    key: operation.key
                }
            },
            primary: {
                leaf: { split: 64, merge: 32 },
                branch: { split: 64, merge: 32 },
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
            const iterator = amalgamator.iterator(snapshots[0], 'forward', null, true)[Symbol.asyncIterator]()

            okay(await iterator.next(), { done: true, value: null }, 'empty')

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
            for await (const items of amalgamator.iterator(snapshots[0], 'forward', Buffer.from('a'), true)) {
                for (const item of items) {
                    gather.push(item.parts[1].toString(), item.parts[2].toString())
                }
            }
            okay(gather, [ 'a', 'A', 'c', 'C' ], 'forward iterator')

            gather.length = 0
            for await (const items of amalgamator.iterator(snapshots[0], 'forward', Buffer.from('a'), false)) {
                for (const item of items) {
                    gather.push(item.parts[1].toString(), item.parts[2].toString())
                }
            }
            okay(gather, [ 'c', 'C' ], 'forward iterator not inclusive')

            gather.length = 0
            for await (const items of amalgamator.iterator(snapshots[0], 'reverse', null, true)) {
                for (const item of items) {
                    gather.push(item.parts[1].toString(), item.parts[2].toString())
                }
            }
            okay(gather, [ 'c', 'C', 'a', 'A' ], 'reverse iterator')

            gather.length = 0
            for await (const items of amalgamator.iterator(snapshots[0], 'reverse', Buffer.from('c'), false)) {
                for (const item of items) {
                    gather.push(item.parts[1].toString(), item.parts[2].toString())
                }
            }
            okay(gather, [ 'a', 'A' ], 'reverse iterator not inclusive')

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

            for await (const items of amalgamator.iterator(snapshots[0], 'forward', null, true)) {
                for (const item of items) {
                    gather.push(item.parts[1].toString(), item.parts[2].toString())
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

    return

    {
        const amalgamator = createAmalgamator()

        await Destructible.rescue(async function () {
            await amalgamator.ready

            const gather = []

            const versions = { 0: true }

            await amalgamator.count()

            amalgamator.locker

            for await (const items of amalgamator.iterator(versions, 'forward', null, true)) {
                for (const item of items) {
                    gather.push(item.parts[1].toString(), item.parts[2].toString())
                }
            }

            okay(gather, [
                'n', 'N', 'o', 'O', 'p', 'P',
                'q', 'Q', 'r', 'R', 's', 'S',
                't', 'T', 'u', 'U', 'v', 'V',
                'w', 'W', 'x', 'X', 'y', 'Y',
                'z', 'Z'
            ], 'amalgamate many reopen')
        })

        await amalgamator.destructible.destroy().rejected
    }

    return

    {
        await fs.rmdir(directory, { recursive: true })

        const amalgamator = createAmalgamator()

        await amalgamator.ready

        const versions = { 0: true }

        const iterator = amalgamator.iterator(versions, 'forward', null, true)

        {
            const version = 1
            const versions = { 0: true }
            versions[version] = true
            const batch = put.concat(del).concat(put).concat(del).concat(put)
            const mutator = amalgamator.mutator(versions, version)
            await mutator.merge(batch)
            okay(!mutator.conflicted, 'not conflicted')
            mutator.commit()
        }

        {
            await new Promise(resolve => setTimeout(resolve, 250))
            const version = 2
            const versions = { 0: true }
            versions[version] = true
            const batch = put.concat(del).concat(put).concat(del).concat(put)
            const mutator = amalgamator.mutator(versions, version)
            await mutator.merge(batch)
            okay(mutator.conflicted, 'conflicted next stage')
            mutator.commit()
        }

        iterator['return']()

        {
            await amalgamator.drain()
            console.log(require('util').inspect(amalgamator.status, { depth: null }))
            const status = amalgamator.status
            const version = 3
            const versions = { 0: true, 2: true, 1: true }
            versions[version] = true
            const mutator = amalgamator.mutator(versions, version)
            await mutator.merge(put)
            okay(!mutator.conflicted, 'not conflicted')
            mutator.commit()
        }

        {
            const version = 4
            const versions = { 0: true }
            versions[version] = true
            const mutator = amalgamator.mutator(versions, version)
            await mutator.merge(del)
            okay(mutator.conflicted, 'conflicted within stage')
            mutator.commit()
        }

        {
            await amalgamator.amalgamate({ 0: true, 3: true, 4: true })
            console.log(require('util').inspect(amalgamator.status, { depth: null }))
            const version = 5
            const versions = { 0: true, 5: true }
            const mutator = amalgamator.mutator(versions, version)
            await mutator.merge(put.concat(put).concat(put).concat(put).concat(put))
            okay(!mutator.conflicted, 'not conflicted before rollback')
            mutator.rollback()
            await amalgamator.drain()
            console.log(require('util').inspect(amalgamator.status, { depth: null }))
            const status = amalgamator.status

            const gather = []

            for await (const items of amalgamator.iterator({ 0: true }, 'forward', null, true)) {
                for (const item of items) {
                    gather.push(item.parts[1].toString(), item.parts[2].toString())
                }
            }

            okay({
                gather: gather,
                length: status.stages.length,
                count: status.stages[0].count
            }, {
                gather: [],
                length: 1,
                count: 0
            }, 'amalgamate many reopen')
        }

        await amalgamator.destructible.destroy().rejected
    }
})
