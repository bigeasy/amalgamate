require('./proof')(2, function (step, serialize, deepEqual, Strata, tmp, gather) {
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
    step(function () {
        names.forEach(step([], function (name) {
            step(function () {
                fs.mkdir(tmp + '/' + name, step())
            }, function () {
                serialize(__dirname + '/fixtures/' + name + '.json', tmp + '/' + name, step())
            }, function () {
                var strata = new Strata({
                    extractor: revise.extractor(extractor),
                    comparator: revise.comparator(comparator),
                    leafSize: 3, branchSize: 3,
                    directory: tmp + '/' + name
                })
                step(function () {
                    strata.open(step())
                }, function () {
                    return strata
                })
            })
        }))
    }, function (stratas) {
        var primary = stratas.shift()
        step(function () {
            var versions = {}
            '0 1 2 3 4'.split(/\s/).forEach(function (version) {
                versions[version] = true
            })
            stratas.forEach(step([], function (strata) {
                skip.forward(strata, comparator, versions, {}, 'a', step())
            }))
        }, function (iterators) {
            designate.forward(comparator, function () { return false }, iterators, step())
        }, function (iterator) {
            step(function () {
                amalgamate.amalgamate(deleted, primary, iterator, step())
            }, function () {
                iterator.unlock()
                gather(step, primary)
            })
        }, function (records) {
            var versions
            versions = records.map(function (record) { return record.version })
            records = records.map(function (record) { return record.value })
            deepEqual(records, [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'i' ], 'records')
            deepEqual(versions, [ 0, 0, 0, 0, 0, 0, 0, 0 ], 'versions')
        }, function () {
            step(function (strata) {
                strata.close(step())
            })(stratas.concat(primary))
        })
    })
})
