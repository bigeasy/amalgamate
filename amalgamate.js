var splice = require('splice')
var twiddle = require('twiddle')

module.exports = function (deleted, version, primary, iterator, callback) {
    function revise (object) {
        if (object) {
            var revised = {}
            for (var key in object) revised[key] = object[key]
            revised.version = version
            return revised
        }
    }
    splice(function (incoming, existing) {
        return deleted(incoming.record, incoming.key) ? 'delete' : 'insert'
    }, primary, twiddle(iterator, function (record, key, callback) {
        callback(null, revise(record), revise(key))
    }), callback)
}
