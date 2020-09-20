module.exports = function (outer, inner) {
    for (const key in inner) {
        if (!outer[key]) {
            return false
        }
    }
    return true
}
