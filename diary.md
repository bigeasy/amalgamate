## Sat Nov  7 06:08:12 CST 2020

Increasingly tempting to require compound keys. Running into it again. Would
like some way to insist on compound keys and a way of specifying a partial
compare. Compound keys are only required for records that go through designate,
though, and then you have the problem of nesting the partialiaty.

## Fri Oct 30 04:45:41 CDT 2020

It is really tempting to say that Amalgamate will always have compound keys and
a key that is not compound is a key of one element. This would be a penalty for
locket which only ever has a `Buffer` as a key, and then the penalty would be
incurred especially if the database was only ever read but would simplify things
otherwise by using ascension and then using its partial comparison for the inner
where records would always be stored as arrays. Searches would always be wrapped
in an array, etc.

In fact, it makes sense to do something like this partially for now since we
have to always twiddle the results coming out of primary to use them to do our
designation. We can just wrap the key in the search or get.

## Sat Sep 26 20:40:19 CDT 2020

New shutdown problems. Same as before, but now with a new level of indirection.
When we call destroy on an Amalgamator a Locker may be in the process of
rotating and then call one of our rotation steps. We'll have no idea if it is
coming. An application can wait for rotation to drain before destroying, but
destroying is supposed to come from the user, so it will have to stave off
destruction of the Amalgamators, wait for the Locker to drain, then it can
destroy Amalgamators and they can just destroy their Stratas immediately.

But, this sort of timing, is it necessary. The working method could report all
the time, but we'd need to document it.

`working()` should only be used for operations that will proceded after shutdown
to finalize an object, the most obvious case is any sort of work queue. The
queue should no longer allow new work to be submitted, working will fire to
indicate that progress is being made toward shutdown by doing work in the queue.

And isn't this the case, finish what you where doing is going to be the common
case for my applications. Maybe at some point there will be an application that
has a specific shutdown tasks that begins at shutdown, such as saving state by
writing out a lot of data to disk.

Somehow I see some sort of shutdown dependency tree, like the Locker ->
Amalgamate -> Strata tree in this application. I also see queues, or boundaries.
Strata when given work needs to write it and then maybe balance it. If you stop
giving it work, it will finish writing and balancing. Amalgamate when is asked
to write to stage or rotate stage. If you stop writing to stage there will be no
more rotating of stages, but rotating of stages requires giving writes to
Strata, so there really is no way for the root destroy to tell Strata to
destroy itself, it has no right.

And what then if there is an error? I suppose with an error we attempt to
shutdown as many brances of the tree as will shutdown in an orderly fashion.

Is this a reasonable model for shutdown? Every destructible thing is a queue in
some fashion, it has input and output and either the user is responsible for
halting input, or else provide a logic for it. It seems like there ought to be
an operational check at the boundary of Memento that will not allow new mutators
and snapshots to be created, and that would make it so that Amalgamate doesn't
have to duplicate this checking in merge, nor does Strata need to perform this
at `insert` and `remove`, but Destructible still complains about creating a new
`ephemeral`, that is safety brake.

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
