require('./proof')(2, prove)

function prove (async, assert) {
    var amalgamate = require('../..')
    var designate = require('designate')
    var skip = require('skip')
    var revise = require('revise')
    var fs = require('fs')
    function deleted (record) {
        return record.deleted
    }
    function extractor (record) {
        return record.value
    }
    function comparator (a, b) {
        return a < b ? -1 : a > b ? 1 : 0
    }
    var names = 'primary one two three'.split(/\s+/)
    async(function () {
        async.map(function (name) {
            async(function () {
                fs.mkdir(tmp + '/' + name, async())
            }, function () {
                serialize(__dirname + '/fixtures/' + name + '.json', tmp + '/' + name, async())
            }, function () {
                var strata = new Strata({
                    extractor: revise.extractor(extractor),
                    comparator: revise.comparator(comparator),
                    leafSize: 3, branchSize: 3,
                    directory: tmp + '/' + name
                })
                async(function () {
                    strata.open(async())
                }, function () {
                    return strata
                })
            })
        })(names)
    }, function (stratas) {
        var primary = stratas.shift()
        async(function () {
            var versions = {}
            '0 1 2 3 4'.split(/\s/).forEach(function (version) {
                versions[version] = true
            })
            console.log(stratas)
            async.map(function (strata) {
                skip.forward(strata, comparator, versions, {}, 'a', async())
            })(stratas)
        }, function (iterators) {
            console.log(iterators)
            designate.forward(comparator, function () { return false }, iterators, async())
        }, function (iterator) {
            async(function () {
                amalgamate(deleted, 0, primary, iterator, async())
            }, function () {
                iterator.unlock(async())
            }, function () {
                gather(primary, async())
            })
        }, function (records) {
            var versions
            versions = records.map(function (record) { return record.version })
            records = records.map(function (record) { return record.value })
            assert(records, [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'i' ], 'records')
            assert(versions, [ 0, 0, 0, 0, 0, 0, 0, 0 ], 'versions')
        }, function () {
            async.forEach(function (strata) {
                strata.close(async())
            })(stratas.concat(primary))
        })
    })
}
