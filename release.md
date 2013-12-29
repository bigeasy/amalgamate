### Parameterize Primary Version

The primary version number of the primary tree is now configurable. Now, in
addition to using Amalgamate to merge a collection of staging trees into a
primary tree, I can use Amalgamate to merge a collection of in-memory records
into a staging tree, thereby implementing a batch insert or delete of records
into a staging tree.

### Upgraded Dependent Libraries

Upgraded the dependent libraries that had shifted their interfaces out from
under Amalgamate.

I replaced MVCC with Revise. MVCC is going to become a module that is a
collection of other modules, so the last of its bits moved to Revise.

Skip is upgraded for visited version accumulation while Designate is upgraded
mainly because it is newer and possibly better.

### Issue by Issue

 * Upgrade Desginate to 0.0.1. #16.
 * Replace MVCC with Revise. #15.
 * Upgrade Skip to 0.0.3. #14.
