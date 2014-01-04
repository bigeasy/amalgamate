### Upgrade Splice

Splice 0.0.2 fixes a bug where Splice will attempt to insert a record into the
zero index of a leaf page which is an error for any leaf page other than the
left-most leaf page.

### Issue by Issue

 * Upgrade Splice to 0.0.2. #25.
