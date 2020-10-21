const assert = require('assert')
const events = require('events')
const noop = require('nop')

class Locker extends events.EventEmitter {
    constructor ({ heft }) {
        super()
        this._heft = [{ max: heft }]
        this._group = 1
        this._version = 1
        this._order = 0
        this._completed = 0
        this._amalgamators = new Set
        this._rotating = false
        this._groups = []
        this._unshift(this._group++)
        this._unstaged = []
        this._references = 0
        this._zeroed = null
    }

    register (amalgamator) {
        this._amalgamators.add(amalgamator)
        return this._groups[0].group
    }

    unregister (amalgamator) {
        this._amalgamators.delete(amalgamator)
    }

    recover (versions) {
        assert(this._groups[0].group == 1 && this._groups[0].heft == 0)
        for (const [ version, committed ] of versions) {
            const mutation = {
                version, created: 0, completed: 0, rolledback: ! committed
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
            return index == 0 || group.amalgamated.size != this._amalgamators.size
        })
        groups.forEach((group, index) => group.references[index]++)
        return { groups, completed: this._completed  }
    }

    mutator () {
        assert(this._shutdown == null)
        const mutation = {
            version: this._version++,
            created: this._order++,
            order: 0,
            completed: Number.MAX_SAFE_INTEGER,
            rolledback: false
        }
        this._groups[0].max = mutation.version
        this._groups[0].mutations.set(mutation.version, mutation)
        return { ...this.snapshot(), mutation, conflicted: false }
    }

    visible (version, { completed, mutation: current = null }) {
        if (version == 0 || (current != null && current.version == version)) {
            return true
        }
        const group = this._groupByVersion(version)
        const mutation = group.mutations.get(version)
        assert(mutation)
        return mutation.completed <= completed && ! mutation.rolledback
    }

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
            this._groups.length == 1 &&
            this._groups[0].state == 'appending' &&
            this._heft[0].max < this._groups[0].heft
        ) {
            while (this._heft.length != 1) {
                this._unstaged.push(this._heft.shift())
            }
            this._groups[0].state = 'rotating'
            for (const amalgamator of this._amalgamators) {
                amalgamator.rotate(this._group)
            }
        }
    }

    _groupByVersion (version) {
        return this._groups.filter(group => group.min < version && version <= group.max)[0]
    }

    group (version) {
        return this._groupByVersion(version).group
    }

    _unshift (group) {
        this._groups.unshift({
            state: 'appending',
            group: group,
            min: this._version,
            max: this._version,
            mutations: new Map,
            rotated: new Set,
            amalgamated: new Set,
            unstaged: new Set,
            references: [ 0, 0 ],
            heft: 0
        })
        this._version++
    }

    rotated (amalgamator) {
        assert.equal(this._groups.length, 1)
        assert(!this._groups[0].rotated.has(amalgamator))
        this._groups[0].rotated.add(amalgamator)
        if (this._groups[0].rotated.size == this._amalgamators.size) {
            this._groups[0].state = 'rotated'
            this._groups[0].max = this._version
            this._unshift(this._group++)
            this._maybeAmalgamate()
        }
    }

    _maybeAmalgamate () {
        if (
            this._groups.length == 2 &&
            this._groups[1].state == 'rotated' &&
            this._groups[1].references[0] == 0
        ) {
            this._groups[1].state == 'amalgamating'
            // TODO Is this okay? No one is editing.
            const mutator = { completed: this._completed  }
            for (const amalgamator of this._amalgamators) {
                amalgamator.amalgamate(mutator)
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
            this._groups[1].references[1] == 0
        ) {
            this._groups[1].state = 'unstaging'
            for (const amalgamator of this._amalgamators) {
                amalgamator.unstage()
            }
        }
    }

    unstaged (amalgamator) {
        assert(!this._groups[1].unstaged.has(amalgamator))
        this._groups[1].unstaged.add(amalgamator)
        if (this._groups[1].unstaged.size == this._amalgamators.size) {
            this._groups.pop()
            while (this._unstaged.length != 0) {
                this._unstaged.shift().resolve.call()
            }
            this._maybeRotate()
        }
    }

    release ({ groups }) {
        groups.forEach((group, index) => group.references[index]--)
        this._maybeAmalgamate()
        this._maybeUnstage()
        if (--this._references == 0 && this._zeroed != null) {
            this._zeroed.resolve()
            this._zeroed = null
        }
    }

    heft (version, heft) {
        this._groupByVersion(version).heft += heft
        this._maybeRotate()
    }

    rotate () {
        return new Promise(resolve => {
            if (this._amalgamators.size == 0) {
                resolve()
            } else {
                this._heft.unshift({ max: -1, resolve })
                this._maybeRotate()
            }
        })
    }

    async drain () {
        if (this._references != 0) {
            if (this._zeroed == null) {
                this._zeroed = { promise: null, resolve: null }
                this._zeroed.promise = new Promise(resolve => this._zeroed.resolve = resolve)
            }
            await this._zeroed.promise
        }
    }
}

module.exports = Locker
