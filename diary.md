## Tue Sep 15 14:44:14 CDT 2020

Starting to consider opportunistic locking, which does not apply to Locket. Who
ever ran last ought to win, although I'll have to think about whether that rule
can be defeated by the current implementation as well. Perhaps strictly yes, but
practically the user shouldn't be able to tell the difference without adding
instrumentation and the effect is the same.

However, in Mememto versions 36 and 37 are outstanding. 37 inserts enough
records into a store that a new stage is created. Now 36 comes along and inserts
records. Assuming that the greatest version wins, and exception is raised if
there is a newer version of a record, 36 will be inserting into an empty stage
and the 37 inserts will be in a stage in the process of amalgamation.

We run into a problem where amalgamation erases version numbers. This always
gives you pause, but you can always remedy this by simply holding onto the
stage, prevent amalgamation until you're certain that the version number no
longer matters and it can just be that which always was.

So, let's add 38 to the mix, it is outstanding as well. It is looking at the
same store. 38 wins over both 37 and 36. 36 looses to 37, so 36 is going to have
to know that it is supposed to check the amalgamating store as well as its
immediate store for each record it inserts to see if there is a newer version.
Since this going to occur only during a transitional period, we don't have to
optimize this behavior overly much, we can straight up search each record we
insert in the amalgamating stage, get cursor, binary search, release cursor.

One improvement on this would be to partition the leaves such that we are
certain that all the versions of a record on in the same leaf. It would greatly
simplify the query logic to do so. This is something that keeps suggesting
itself, so not it is time to add this to Strata. It ought to just be there.

When it arrives it can check to see if the greatest version in the amalgamating
store is greater than 37, it is, and add a shared reference to that store so
although it will be amalgamated, it will not be deleted. We can still reference
it.

But, 37 may get completely amalgamated and have no other reader references, and
36 hasn't gotten around to reading this store yet. Now we need some way to say
to Amalgamate, don't let go of this amalgamated version because its greatest
version is 37 and you may not know about it, but there is a version 36 out there
and it may come around and need to read reference this this store.

We could fire a synchronous amalgamated event that Memento could hook. When
called it checks outstanding versions, if one is less than the versions
amalgamated, it would grab a read reference somehow, let's say instead of
creating an iterator, we now add a reference concept that will do what an
iterator does to hold references, and the iterator will use it.

It holds that reference and then there's another map of functions that are
called when the version commits or rolls back. Map of verisons to array of
functions, and this will make sure that the stage exists and cannot be unstaged.

This give-me-a-reference function would have to have a flag that says, yes, I
want a refernece all of them, even amalgamated ones.

All of this logic is so terribly internal to Memento and IndexedDB, it is going
to be a nightmare to document.
