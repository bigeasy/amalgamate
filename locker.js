// # Locker

// Node.js API.
const assert = require('assert')

// A `Promise` wrapper that captures `resolve` and `reject`.
const Future = require('perhaps')

class Locker {
    constructor ({ mutations, version, size }) {
        this._size = [{ max: size }]
        this._groups = []
        this._unshift(0, 0, version + 1, mutations)
        this._group = this._groups[0].max + 1
        this._version = version + 1
        this._order = 1
        this._completed = 0
        this._unstaged = []
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
    //

    // Amalgamators may have many stages but there are only ever at most two
    // groups.
    //
    // The group references array indicates whether the reference was
    // potentially a write reference. We increment the reference count in the
    // two element reference array according to the order of the group in a copy
    // of the group array at the time of the snapshot. After we've shifted a new
    // group onto the groups array, the old group will see its reference count
    // in the second element fluctuate, while the reference count in the first
    // element will only decrement. When it reaches zero the old group can be
    // amalgamated.
    //
    // We exclude a group from a snapshot when it has been amalgamated. It is
    // still present while it awaits rotation. By excluding it, the second
    // element of the reference count array will only decrement. When it reaches
    // zero the amalgamated group can be popped.

    //
    snapshot () {
        const groups = this._groups.filter((group, index) => {
            return index == 0 || ! group.amalgamated
        })
        groups.forEach((group, index) => group.references[index]++)
        return { groups, created: this._completed  }
    }
    //

    // The mutation with its nested `completed` that is uncompleted, therefore
    // `MAX_SAFE_INTEGER`, not to be confused with the one we're merging from
    // the `snapshot()`.

    // A snapshot with an additional nested mutation property.

    // `order` is used by amalgamate to order the appends for the given version.
    // It is a sub-version.

    //
    mutator () {
        const mutation = {
            version: this._version++,
            order: 0,
            completed: Number.MAX_SAFE_INTEGER,
            competitors: new Set,
            rolledback: false,
            conflicted: false
        }
        this._groups[0].max = mutation.version
        this._groups[0].mutations.set(mutation.version, mutation)
        return { ...this.snapshot(), mutation }
    }
    //

    // * A zero version indicates the record came from the primary tree and is
    // therefore visible.
    // * A `MAX_SAFE_INTEGER` version indicates a user provided iterator that
    // has in-memory overrides.
    // * Our own changes are always visible even if we've rolled back.
    // * A staged version is visible if it was completed at or before our
    // snapshot's creation and it was not rolled back.

    //
    visible (version, { created, mutation }) {
        if (
            version == 0 ||
            version == Number.MAX_SAFE_INTEGER ||
            (mutation != null && mutation.version == version)
        ) {
            return true
        } else {
            const group = this._groupByVersion(version)
            const mutation = group.mutations.get(version)
            assert(mutation)
            return mutation.completed <= created && ! mutation.rolledback
        }
    }
    //

    // Upon detecting a conflict The `conflicted` flag is set but it is only for
    // the caller's edification. We only use the `rolledback` flag inside
    // `Locker`.
    //
    // The first mutation to _detect_ a conflict rolls _itself_ back.
    //
    // Conflict detection is synchronous. (See `amalgamate.js`.)
    //
    // Our mutation is identified by a `version`. Our competitor is also
    // a mutation indentified by a `version`.
    //
    // The presence of two versions of a record in a stage tree page indicate
    // the potential for conflict and requires conflict resoution.
    //
    // If we detect a conflict we rollback immediately.
    //
    // If our mutation has has already rolled back due to a previous conflict we
    // are conflicted. (At this point, however, we only call this function once
    // just prior to commit.)
    //
    // If the given competitor version is the version of our mutation we are not
    // conflicted, i.e. we do not conflict with ourselves.
    //
    // The competitor mutation completed before we were created we are not
    // conflicted.
    //
    // If the competitor mutation has not completed or completed after our
    // mutation and it was not rolled back we are conflicted.

    //
    conflicted ({ created, mutation }) {
        if (mutation.conflicted) {
            return true
        }
        for (const version of mutation.competitors) {
            if (mutation.version != version) {
                const group = this._groupByVersion(version)
                const competitor = group.mutations.get(version)
                assert(competitor)
                if (completed.completed > created && ! competitor.rolledback) {
                    mutation.conflicted = true
                    mutation.rolledback = true
                    break
                }
            }
        }
        return mutation.conflicted
    }
    //

    // Our write will be synchronous as will our in-memory commit. If we do not
    // actually write our commit, if the append fails to write, any commit that
    // follows this commit will also fail to write.

    //
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
    }
    //

    // The heft property of the locker is an array and we temporarily shift a
    // negative maximum heft onto the array to force a rotate the next time we
    // check. If we are currently rotating we will immediately rotate when
    // finished. The `done` future is separate form the ones used to drain.

    //
    rotate () {
        const done = new Future
        this._size.unshift({ max: -1, done })
        this._maybeRotate()
        return done
    }
}

module.exports = Locker
