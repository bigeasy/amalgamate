require('proof')(1, async okay => {
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
                key: encode(operation.key),
                value: ('value' in operation) ? encode(operation.value) : null
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
    destructible.destroy()
    await destructible.rejected

    okay('done')
})
