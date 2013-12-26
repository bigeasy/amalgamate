var cadence = require('cadence')
var ok = require('assert').ok

function Amalgamator (deleted, primary, iterator) {
    this._deleted = deleted
    this._primary = primary
    this._iterator = iterator
}

Amalgamator.prototype.amalgamate = cadence(function (step) {
    var insert
    step(function () {
        this._iterator.next(step())
    }, function (record, key) {
        if (record && key) insert = step(function () {
           key.version = record.version = 0
           if (!this._mutator) {this._primary.mutator(key, step(step, function ($) {
                this._mutator = $
                return this._mutator.index
            }))} else {
                this._mutator.indexOf(key, step())
            }
        }, function (index) {
            if (index < 0) return ~ index
            else step(function () {
                this._mutator.remove(index, step())
            }, function () {
                this._mutator.indexOf(key, step())
            })
        }, function (index) {
            if (!this._deleted(record)) step(function () {
                this._mutator.insert(record, key, index, step())
            }, function (result) {
                if (result != 0) {
                    ok(result > 0, 'went backwards')
                    this._mutator.unlock()
                    delete this._mutator
                    step(insert)
                }
            })
        })(1)
        else step(null)
    })()
})

Amalgamator.prototype.unlock = function () {
    if (this._mutator) this._mutator.unlock()
}

exports.amalgamate = cadence(function (step, deleted, primary, iterator) {
    var amalgamator = new Amalgamator(deleted, primary, iterator)
    step(function () {
        amalgamator.amalgamate(step())
    }, function () {
        amalgamator.unlock()
    })
})
