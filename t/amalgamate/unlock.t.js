require('./proof')(1, function (step, serialize, deepEqual, Strata, tmp, gather) {
    var amalgamate = require('../..')
    var designate = require('designate')
    var skip = require('skip')
    var mvcc = require('mvcc')
    var fs = require('fs')
    function deleted () {
        return false
    }
    function extractor (record) {
        return record.value
    }
    function comparator (a, b) {
        return a < b ? -1 : a > b ? 1 : 0
    }
    function Bogus () {
    }
    Bogus.prototype.mutator = function (key, callback) {
        callback(new Error('bogus'))
    }
    var names = 'one two three'.split(/\s+/)
    step(function () {
        names.forEach(step([], function (name) {
            step(function () {
                fs.mkdir(tmp + '/' + name, step())
            }, function () {
                serialize(__dirname + '/fixtures/' + name + '.json', tmp + '/' + name, step())
            }, function () {
                var strata = new Strata({
                    extractor: mvcc.extractor(extractor),
                    comparator: mvcc.comparator(comparator),
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
        var primary = new Bogus()
        step(function () {
            stratas.forEach(step([], function (strata) {
                skip.forward(strata, comparator, { 0: true }, 'a', step())
            }))
        }, function (iterators) {
            designate.forward(comparator, deleted, iterators, step())
        }, function (iterator) {
            step([function () {
                amalgamate.amalgamate(deleted, primary, iterator, step())
            }, function (_, error) {
                deepEqual(error.message, 'bogus', 'caught error')
            }], function () {
                iterator.unlock()
            })
        }, function () {
            step(function (strata) {
                strata.close(step())
            })(stratas)
        })
    })
})
