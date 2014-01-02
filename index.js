var cadence = require('cadence')
var ok = require('assert').ok

function Amalgamator (deleted, version, primary, iterator) {
    this._deleted = deleted
    this._version = version
    this._primary = primary
    this._iterator = iterator
}

Amalgamator.prototype.amalgamate = cadence(function (step) {
    var insert
    step(function () {
        this._iterator.next(step())
    }, function (record, key) {
        if (record && key) insert = step(function () {
           key.version = record.version = this._version
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

module.exports = cadence(function (step, deleted, version, primary, iterator) {
    var amalgamator = new Amalgamator(deleted, version, primary, iterator)
    step([function () {
        amalgamator.unlock()
    }], function () {
        amalgamator.amalgamate(step())
    })
})
