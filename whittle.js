module.exports = function (comparator, whittle) {
    return function (left, right) {
        return comparator(whittle(left), whittle(right))
    }
}
