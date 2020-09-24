const assert = require('assert')

class Locker {
    constructor ({ heft }) {
        this._heft = heft
        this._group = 0
        this._version = 1
        this._order = 0
        this._completed = 0
        this._amalgamators = new Set
        this._rotating = false
        this._groups = [{
            state: 'appending',
            group: this._group++,
            min: this._version,
            max: Number.MAX_SAFE_INTEGER,
            mutations: new Map,
            rotated: new Set,
            amalgamated: new Set,
            unstaged: new Set,
            references: [ 0, 0 ],
            heft: 0
        }]
    }

    register (amalgamator) {
        this._amalgamators.add(amalgamator)
        return this._groups[0].group
    }

    snapshot () {
        const groups = this._groups.filter((group, index) => {
            return index == 0 || group.amalgamated.size != this._amalgamators.size
        })
        groups.forEach((group, index) => group.references[index]++)
        return { groups, completed: this._completed  }
    }

    mutator () {
        const mutation = {
            version: this._version++,
            created: this._order++,
            completed: Number.MAX_SAFE_INTEGER,
            rolledback: false
        }
        this._groups[0].mutations.set(mutation.version, mutation)
        return { ...this.snapshot(), mutation }
    }

    visible (version, { completed }) {
        if (version == 0) {
            return true
        }
        const group = this._groupByVersion(version)
        const mutation = group.mutations.get(version)
        assert(mutation)
        return mutation.completed <= completed && ! mutation.rolledback
    }

    conflicted (version, { completed }) {
        const group = this._groupByVersion(version)
        const mutation = group.mutations.get(version)
        console.log(mutation, completed)
        assert(mutation)
        return mutation.completed > completed && ! mutation.rolledback
    }

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
                max: this._heft,
                total: this._groups.reduce((sum, group) => sum + group.heft, 0)
            },
            rotating: this._rotating,
            version: this._version,
            group: this._group,
            completed: this._completed,
            amalgamators: this._amalgamators.size,
            groups: this._groups.map(group => {
                const mutations = []
                for (const mutation of group.mutations.values()) {
                    mutations.push(mutation)
                }
                return {
                    ...group, mutations,
                    rotated: group.rotated.size,
                    amalgamated: group.amalgamated.size,
                    unstaged: group.unstaged.size
                }
            })
        }
    }

    _maybeRotate () {
        if (
            ! this._rotating &&
            this._groups.length ==  1 &&
            this._heft < this._groups.reduce((sum, group) => sum + group.heft, 0)
        ) {
            this._rotating = true
            this._groups[0].state = 'rotating'
            for (const amalgamator of this._amalgamators) {
                amalgamator.rotate(this._group)
            }
        }
    }

    _groupByVersion (version) {
        return this._groups.filter(group => group.min <= version && version < group.max)[0]
    }

    group (version) {
        return this._groupByVersion(version).group
    }

    rotated (amalgamator) {
        assert.equal(this._groups.length, 1)
        assert(!this._groups[0].rotated.has(amalgamator))
        this._groups[0].rotated.add(amalgamator)
        if (this._groups[0].rotated.size == this._amalgamators.size) {
            this._rotating = false
            this._groups.unshift({
                state: 'appending',
                group: this._group++,
                min: this._groups[0].max = this._version,
                max: Number.MAX_SAFE_INTEGER,
                mutations: new Map,
                rotated: new Set,
                amalgamated: new Set,
                unstaged: new Set,
                references: [ 0, 0 ],
                heft: 0
            })
            this._maybeAmalgamate()
        }
    }

    _maybeAmalgamate () {
        if (
            this._groups.length == 2 &&
            this._groups[1].state == 'rotating' &&
            this._groups[1].rotated.size == this._amalgamators.size &&
            this._groups[1].references[0] == 0
        ) {
            this._groups[1].state == 'amalgamating'
            for (const amalgamator of this._amalgamators) {
                amalgamator.amalgamate()
            }
        }
    }

    amalgamated (amalgamator) {
        assert(!this._groups[1].amalgamated.has(amalgamator))
        this._groups[1].amalgamated.add(amalgamator)
        if (this._groups[1].amalgamated.size == this._amalgamators.size) {
            this._groups[1].state = 'amalgamated'
            this._maybeUnstage()
        }
    }

    _maybeUnstage () {
        if (
            this._groups.length == 2 &&
            this._groups[1].state == 'amalgamated' &&
            this._groups[1].amalgamated.size == this._amalgamators.size &&
            this._groups[1].references[1] == 0
        ) {
            this._groups[1].state == 'unstaging'
            for (const amalgamator of this._amalgamators) {
                amalgamator.unstage()
            }
        }
    }

    unstage (amalgamator) {
        assert(!this._groups[1].unstaged.has(amalgamator))
        this._groups[1].unstaged.add(amalgamator)
        if (this._groups[1].unstaged.size == this._amalgamators.size) {
            this._groups.pop()
            this._maybeRotate()
        }
    }

    release ({ groups }) {
        groups.forEach((group, index) => group.references[index]--)
        this._maybeAmalgamate()
        this._maybeUnstage()
    }

    heft (version, heft) {
        this._groupByVersion(version).heft += heft
        this._maybeRotate()
    }
}

module.exports = Locker
