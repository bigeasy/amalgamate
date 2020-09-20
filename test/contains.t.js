require('proof')(2, okay => {
    const contains = require('../contains')
    okay(contains({ a: true, b: true }, { a: true }), 'contains')
    okay(!contains({ a: true, b: true }, { c: true }), 'does not contain')
})
