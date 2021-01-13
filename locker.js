const assert = require('assert')

const Future = require('perhaps')

class Locker {
    constructor ({ mutations, version, size }) {
        this._size = [{ max: size }]
        this._groups = []
        this._unshift(0, 0, version + 1, mutations)
        this._group = this._groups[0].max + 1
        this._version = version + 1
        this._order = 1
        this._unstaged = []
        this._references = 0
        this._zeroed = Future.resolve()
        this.destroyed = false
        this.position = 0
        this.rotating = new Future
        this.amalgamating = new Future
        this.unstaging = new Future
    }

    destroy () {
        this.destroyed = true
        this.rotating.resolve(null)
    }

    get latest () {
        return this._groups[0].group
    }

    advance (position) {
        this.position = position
        this._maybeRotate()
    }

    _unshift (group, min, max = min, mutations = new Map) {
        this._groups.unshift({
            state: 'appending',
            group: group,
            // TODO We need rules for min and max. Currently `min` is exclusive
            // and `max` is inclusive. Either both exclusive or inclusive,
            // exclusive like `slice()`.
            min: min,
            max: max,
            mutations: mutations,
            amalgamated: false,
            references: [ 0, 0 ],
            heft: 0
        })
    }

    recover (versions) {
        assert(this._groups[0].group == 1 && this._groups[0].heft == 0)
        for (const [ version, committed ] of versions) {
            const mutation = {
                version, completed: 0, rolledback: ! committed
            }
            this._groups[0].mutations.set(version, mutation)
            this._groups[0].max = Math.max(this._groups[0].max, version)
            this._version = this._groups[0].max + 1
        }
    }

    snapshot () {
        this._references++
        assert(this._shutdown == null)
        const groups = this._groups.filter((group, index) => {
            return index == 0 || ! group.amalgamated
        })
        groups.forEach((group, index) => group.references[index]++)
        return { groups, completed: this._completed  }
    }

    mutator () {
        assert(this._shutdown == null)
        // The mutation with its nested `completed` that is uncompleted,
        // therefore `MAX_SAFE_INTEGER`, not to be confused with the one we're
        // merging from the `snapshot()`.
        const mutation = {
            version: this._version++,
            order: 0,
            completed: Number.MAX_SAFE_INTEGER,
            rolledback: false
        }
        this._groups[0].max = mutation.version
        this._groups[0].mutations.set(mutation.version, mutation)
        return { ...this.snapshot(), mutation, conflicted: false }
    }

    visible (version, { completed, mutation: current = null }) {
        if (
            version == 0 ||
            version == Number.MAX_SAFE_INTEGER ||
            (current != null && current.version == version)
        ) {
            return true
        }
        const group = this._groupByVersion(version)
        const mutation = group.mutations.get(version)
        assert(mutation)
        return mutation.completed <= completed && ! mutation.rolledback
    }

    // Should be called immediately before calling `commit`.
    conflicted (version, { completed, mutation: { version: current } }) {
        if (current == version) {
            return false
        }
        const group = this._groupByVersion(version)
        const mutation = group.mutations.get(version)
        assert(mutation)
        if (mutation.completed > completed && ! mutation.rolledback) {
            mutation.conflicted = true
            return true
        }
        return false
    }

    // Our write will be synchronous as will our in-memory commit. If we do not
    // actually write our commit, if the append fails to write, any commit that
    // follows this commit will also fail to write.
    commit ({ mutation, groups }) {
        mutation.completed = this._completed = this._order++
        this.release({ groups })
    }

    rollback ({ mutation, groups }) {
        mutation.completed = this._completed = this._order++
        mutation.rolledback = true
        this.release({ groups })
    }

    get status () {
        return {
            heft: {
                max: this._heft.max,
                total: this._groups.reduce((sum, group) => sum + group.heft, 0)
            },
            version: this._version,
            group: this._group,
            completed: this._completed,
            groups: this._groups.map(group => {
                const mutations = []
                for (const mutation of group.mutations.values()) {
                    mutations.push(mutation)
                }
                return { ...group, mutations }
            })
        }
    }

    _groupByVersion (version) {
        return this._groups.filter(group => group.min < version && version <= group.max)[0]
    }

    group (version) {
        return this._groupByVersion(version).group
    }

    _maybeRotate () {
        if (
            ! this.destroyed &&
            this._groups.length == 1 &&
            this._groups[0].state == 'appending' &&
            this._size[0].max < this.position
        ) {
            while (this._size.length != 1) {
                this._unstaged.push(this._size.shift())
            }
            this._groups[0].state = 'rotating'
            this.rotating.resolve(this._group)
        }
        return null
    }

    rotated () {
        this._groups[0].state = 'rotated'
        this._groups[0].max = this._version
        this._unshift(this._group++, this._version)
        this._version++
        this.rotating = this.destroyed ? Future.resolve() : new Future
        this._maybeAmalgamate()
    }

    _maybeAmalgamate () {
        if (
            ! this.destroyed &&
            this._groups.length == 2 &&
            this._groups[1].state == 'rotated' &&
            this._groups[1].references[0] == 0
        ) {
            this._groups[1].state = 'amalgamating'
            this.amalgamating.resolve()
        }
    }

    amalgamated () {
        this._groups[1].amalgamated = true
        this._groups[1].state = 'amalgamated'
        this.amalgamating = new Future
        this._maybeUnstage()
    }

    _maybeUnstage () {
        if (
            ! this.destroyed &&
            this._groups.length == 2 &&
            this._groups[1].state == 'amalgamated' &&
            this._groups[1].references[1] == 0
        ) {
            this._groups[1].state = 'unstaging'
            this.unstaging.resolve()
        }
    }

    unstaged () {
        this._groups.pop()
        while (this._unstaged.length) {
            this._unstaged.shift().done.resolve()
        }
        this.unstaging = new Future
        this._maybeRotate()
    }

    release ({ groups }) {
        groups.forEach((group, index) => group.references[index]--)
        this._maybeAmalgamate()
        this._maybeUnstage()
        if (--this._references == 0) {
            this._zeroed.resolve()
        }
    }
    //

    // The heft property of the locker is an array and we temporarily shift a
    // negative maximum heft onto the array to force a rotate the next time we
    // check. If we are currently rotating we will immediately rotate when
    // finished. The `done` future is separate form the ones used to drain.

    //
    rotate () {
        const done = new Future
        this._heft.unshift({ max: -1, done })
        this._maybeRotate()
        return done.future
    }
}

module.exports = Locker
