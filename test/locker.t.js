require('proof')(4, async okay => {
    const fs = require('fs').promises
    const path = require('path')

    const Locker = require('../locker')
    const locker = new Locker({ muatations: new Map, version: 0, size: 1024 })

    const snapshots = [ locker.snapshot() ]

    okay(locker.visible(0, snapshots[0]), 'zero is primary tree and always visible')

    locker.release(snapshots.shift())

    okay(locker.latest, 0, 'group is default zero')
    okay(! locker.rotating.fulfilled, 'would not rotate')
    locker.advance(1025)
    okay(locker.rotating.fulfilled, 'would rotate')

    return

    const recover = new Map
    recover.set(2, true)
    recover.set(3, false)
    locker.recover(recover)

    locker.on('amalgamated', (exclusive, inclusive) => amalgamated.push(exclusive, inclusive))

    function dump () {
        console.log(require('util').inspect(locker.status, { depth: null }))
    }

    const amalgamators = [ new Amalgamator(locker), new Amalgamator(locker) ]

    okay(locker.visible(0, snapshots[0]), 'zero is visible')

    locker.release(snapshots.shift())

    const mutators = [ locker.mutator() ]

    okay(locker.group(mutators[0].mutation.version), 1, 'group lookup')

    snapshots.push(locker.snapshot())

    locker.heft(mutators[0].mutation.version, 16)

    okay(!locker.visible(3, snapshots[0]), 'invisible')

    mutators.push(locker.mutator())

    okay(locker.conflicted(4, mutators[1]), 'conflicted with prior')
    okay(locker.conflicted(5, mutators[0]), 'conflicted with subsequent')

    locker.commit(mutators.shift())

    mutators.push(locker.mutator())

    okay(locker.conflicted(4, mutators[0]), 'still conflicted with prior')
    okay(!locker.conflicted(4, mutators[1]), 'not conflicted with committed')
    okay(locker.conflicted(5, mutators[1]), 'conflicted with uncommitted')

    locker.rollback(mutators.shift())

    okay(!locker.conflicted(4, mutators[0]), 'not conflicted with rolledback')
    okay(!locker.conflicted(6, mutators[0]), 'not conflicted with self')
    okay(locker.visible(6, mutators[0]), 'visible to self')

    okay(!locker.visible(4, snapshots[0]), 'still invisible')

    snapshots.push(locker.snapshot())

    okay(locker.visible(4, snapshots[1]), 'new snapshot visible')

    locker.heft(5, 17)

    snapshots.push(locker.snapshot())

    dump()

    const drained = [ locker.drain(), locker.drain() ]

    locker.commit(mutators.shift())
    locker.release(snapshots.shift())
    locker.release(snapshots.shift())

    snapshots.push(locker.snapshot())

    //dump()

    locker.release(snapshots.shift())
    locker.release(snapshots.shift())

    drained.push(locker.drain())

    for (const drain of drained) {
        await drain
    }

    okay('explicit rotate')

    await locker.rotate()

    okay('done')

    okay(amalgamated, [ 1, 7, 7, 8 ], 'amalgamated')

    //dump()
})
