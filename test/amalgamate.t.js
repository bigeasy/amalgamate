require('proof')(4, async okay => {
    const path = require('path')
    const fs = require('fs').promises

    const Destructible = require('destructible')
    const Amalgamator = require('..')

    const Cache = require('b-tree/cache')

    const directory = path.join(__dirname, 'tmp', 'amalgamate')

    await fs.rmdir(directory, { recursive: true })

    function createAmalgamator () {
        const destructible = new Destructible(1000, 'amalgamate.t')
        return new Amalgamator(destructible, {
            directory: directory,
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
            transformer: function (operation, index) {
                if (operation.type == 'put') {
                    return {
                        method: 'insert',
                        index: index,
                        key: operation.key,
                        parts: [ operation.key, operation.value ]
                    }
                }
                return {
                    method: 'remove',
                    index: index,
                    key: operation.key
                }
            },
            primary: {
                leaf: { split: 64, merge: 32 },
                branch: { split: 64, merge: 32 },
            },
            stage: {
                max: 128 * 8,
                leaf: { split: 64, merge: 32 },
                branch: { split: 64, merge: 32 },
            }
        })
    }

    {
        const amalgamator = createAmalgamator()

        await amalgamator.ready

        await Destructible.rescue(async function () {
            const iterator = amalgamator.iterator({ 0: true }, 'forward', null, true)[Symbol.asyncIterator]()
            okay(await iterator.next(), { done: true, value: null }, 'empty')

            await amalgamator.merge(1, [{
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
            }], 4)

            const gather = []
            const versions = { 0: true, 1: true }
            for await (const items of amalgamator.iterator(versions, 'forward', null, true)) {
                for (const item of items) {
                    gather.push(item.parts[1].toString(), item.parts[2].toString())
                }
            }
            okay(gather, [ 'a', 'A', 'c', 'C' ], 'forward iterator')

            const alphabet = 'abcdefghijklmnopqrstuvwxyz'.split('')

            const put = alphabet.map(letter => {
                return {
                    type: 'put', key: Buffer.from(letter), value: Buffer.from(letter.toUpperCase())
                }
            })
            const del = alphabet.map(letter => {
                return { type: 'del', key: Buffer.from(letter) }
            })

            for (let i = 0; i < 128; i++) {
                const version = i + 1
                const batch = i == 127 ? put.concat(del.slice(0, 13)) : put.concat(del)
                await amalgamator.merge(version, batch, batch.length)
                versions[version] = true
            }

            gather.length = 0

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
            ], 'amalgamate many')


            // TODO Reverse iterator.
        })

        await amalgamator.destructible.destroy().rejected
    }

    {
        const amalgamator = createAmalgamator()

        await Destructible.rescue(async function () {
            await amalgamator.ready

            const gather = []

            const versions = { 0: true, ...(await amalgamator.counted()) }

            await amalgamator.applicable({ 128: true })

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
})
