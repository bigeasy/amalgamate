require('proof')(2, async okay => {
    const path = require('path')
    const fs = require('fs').promises

    const Destructible = require('destructible')
    const Amalgamator = require('..')

    const Cache = require('b-tree/cache')

    const directory = path.join(__dirname, 'tmp', 'amalgamate')

    await fs.rmdir(directory, { recursive: true })

    const destructible = new Destructible('amalgamate.t')
    const amalgamator = new Amalgamator(destructible, {
        directory: directory,
        cache: new Cache,
        comparator: Buffer.compare,
        createIfMissing: true,
        errorIfExists: false,
        header: {
            compose: function (version, method, index, count) {
                return { header: { method, index }, count, version }
            },
            serialize: function (header) {
                return Buffer.from(JSON.stringify({
                    header: {
                        method: header.header.method,
                        index: header.header.index
                    },
                    count: header.count,
                    version: header.version.toString()
                }))
            },
            deserialize: function (buffer) {
                const header = JSON.parse(buffer.toString())
                header.version = BigInt(header.version)
                return header
            }
        },
        transformer: function (operation) {
            return {
                method: operation.type == 'put' ? 'insert' : 'remove',
                key: operation.key,
                value: ('value' in operation) ? operation.value : null
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

    await amalgamator.ready

    const iterator = amalgamator.iterator({ 0: true }, 'forward', null, true)[Symbol.asyncIterator]()
    okay(await iterator.next(), { done: true, value: null }, 'empty')

    await amalgamator.merge(1n, [{
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
    for await (const items of amalgamator.iterator({ 0: true, 1: true }, 'forward', null, true)) {
        for (const item of items) {
            gather.push(item.parts[1].toString(), item.parts[2].toString())
        }
    }
    okay(gather, [ 'a', 'A', 'c', 'C' ], 'forward iterator')

    destructible.destroy()
    await destructible.rejected
})
