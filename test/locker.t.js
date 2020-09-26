require('proof')(11, async okay => {
    const once = require('prospective/once')

    class Amalgamator {
        constructor (locker) {
            this.stages = [{
                group: locker.register(this)
            }]
            this.locker = locker
        }

        rotate (group) {
            this.stages.unshift({ group })
            this.locker.rotated(this)
        }

        amalgamate () {
            this.locker.amalgamated(this)
        }

        unstage () {
            this.locker.unstaged(this)
        }
    }

    const Locker = require('../locker')
    const locker = new Locker({ heft: 32 })

    function dump () {
        console.log(require('util').inspect(locker.status, { depth: null }))
    }

    const amalgamators = [ new Amalgamator(locker), new Amalgamator(locker) ]

    const snapshots = [ locker.snapshot() ]

    okay(locker.visible(0, snapshots[0]), 'zero is visible')

    locker.release(snapshots.shift())

    const mutators = [ locker.mutator() ]

    okay(locker.group(mutators[0].mutation.version), 1, 'group lookup')

    snapshots.push(locker.snapshot())

    locker.heft(mutators[0].mutation.version, 16)

    okay(!locker.visible(2, snapshots[0]), 'invisible')

    mutators.push(locker.mutator())

    okay(locker.conflicted(2, mutators[1]), 'conflicted with prior')
    okay(locker.conflicted(3, mutators[0]), 'conflicted with subsequent')

    locker.commit(mutators.shift())

    mutators.push(locker.mutator())

    okay(locker.conflicted(2, mutators[0]), 'still conflicted with prior')
    okay(!locker.conflicted(2, mutators[1]), 'not conflicted with committed')
    okay(locker.conflicted(3, mutators[1]), 'conflicted with uncommitted')

    locker.rollback(mutators.shift())

    okay(!locker.conflicted(2, mutators[0]), 'not conflicted with rolledback')

    okay(!locker.visible(2, snapshots[0]), 'still invisible')

    snapshots.push(locker.snapshot())

    okay(locker.visible(2, snapshots[1]), 'new snapshot visible')

    locker.heft(3, 17)

    snapshots.push(locker.snapshot())

    //dump()

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

    console.log('explicit rotate')
    await locker.rotate()

    console.log('done')

    //dump()
})
