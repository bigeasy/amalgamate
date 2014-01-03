### Extract Splice Module

Extracted the record merge logic to a module named Splice. Also, created a
module named Twiddle that is a decorator for an MVCC iterator. I'm using it to
rewrite the record and key with a new version.

### Single Function Module

Amalgamate is now a single function module. It returns just the `amalgamate`
function.

### Dependency Upgrades

Upgraded [Skip](https://github.com/bigeasy/skip) and
[Designate](https://github.com/bigeasy/designate).

### Issue by Issue

 * Extract Splice module. #21.
 * Upgrade Designate to 0.0.2. #23.
 * Upgrade Skip to 0.0.4. #22.
 * Module should contain only the one function. #20.
