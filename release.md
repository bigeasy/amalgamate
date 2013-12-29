### Upgraded Dependent Libraries

Upgraded the dependent libraries that had shifted their interfaces out from
under Amalgamate.

I replaced MVCC with Revise. MVCC is going to become a module that is a
collection of other modules, so the last of its bits moved to Revise.

Skip is upgraded for visited version accumulation while Designate is upgraded
mainly because it is newer and possibly better.

### Issue by Issue

 * Replace MVCC with Revise. #15.
