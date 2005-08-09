package Thread::Queue::Duplex;
#
#	Copyright (C) 2005, Presicient Corp., USA
#
#	Provides a means for N threads to queue
#	items to  thread, and then wait only for
#	response to the items the thread queued
#
require 5.008;

use threads;
use threads::shared;
use Thread::Queue::Queueable;

use base qw(Thread::Queue::Queueable);

use strict;
use warnings;

our $VERSION = '0.11';

=head1 NAME

Thread::Queue::Duplex - thread-safe request/response queue with identifiable elements

=head1 SYNOPSIS

	use Thread::Queue::Duplex;
	#
	#	create new queue, and require that there be
	#	registered listeners for an enqueue operation
	#	to succeed, and limit the max pending requests
	#	to 20
	#
	my $q = Thread::Queue::Duplex->new(ListenerRequired => 1, MaxPending => 20);
	#
	#	register as a listener
	#
	$q->listen();
	#
	#	unregister as a listener
	#
	$q->ignore();
	#
	#	wait for a listener to register
	#
	$q->wait_for_listener($timeout);
	#
	#	change the max pending limit
	#
	$q->set_max_pending($limit);
	#
	#	enqueue elements, returning a unique queue ID
	#	(used in the client)
	#
	my $id = $q->enqueue("foo", "bar");
	#
	#	enqueue elements at head of queue, returning a 
	#	unique queue ID (used in the client)
	#
	my $id = $q->enqueue_urgent("foo", "bar");
	#
	#	enqueue elements for simplex operation (no response)
	#	returning the queue object
	#
	$q->enqueue_simplex("foo", "bar");

	$q->enqueue_simplex_urgent("foo", "bar");
	#
	#	dequeue next available element (used in the server),
	#	waiting indefinitely for an element to be made available
	#	returns shared arrayref, first element is unique ID,
	#	which may be undef for simplex requests
	#
	my $foo = $q->dequeue;
	#
	#	dequeue next available element (used in the server),
	#	returns undef if no element immediately available
	#	otherwise, returns shared arrayref, first element is unique ID,
	#	which may be undef for simplex requests
	#
	my $foo = $q->dequeue_nb;
	#
	#	dequeue next available element (used in the server),
	#	returned undef if no element available within $timeout 
	#	seconds; otherwise, returns shared arrayref, first 
	#	element is unique ID, which may be undef for simplex requests
	#
	my $foo = $q->dequeue_until($timeout);
	#
	#	dequeue next available element (used in the server),
	#	but only if marked urgent; otherwise, returns undef
	#
	my $foo = $q->dequeue_urgent();
	#
	#	returns number of items still in queue
	#
	my $left = $q->pending;
	#
	#	maps a response for the
	#	queued element identified by $id;
	#
	$q->respond($id, @list);
	#
	#	tests for a response to the queued 
	#	element identified by $id; returns undef if
	#	not yet available, else returns the queue object
	#
	my $result = $q->ready($id);
	#
	#	returns list of available response ID's;
	#	if list provided, only returns ID's from the list.
	#	Returns undef if none available.
	#	In scalar context, returns only first available;
	#	Else a list of available IDs
	#
	my @ids = $q->available();
	#
	#	wait for and return the response for the 
	#	specified unique identifier
	#	(dequeue_response is alias)
	#
	my $result = $q->wait($id);
	my $result = $q->dequeue_response($id);
	#
	#	waits up to $timeout seconds for a response to 
	#	the queued element identified by $id; returns undef if
	#	not available within $timeout, else returns the queue object
	#
	my $result = $q->wait_until($id, $timeout);
	#
	#	wait for a response to the queued 
	#	elements listed in @ids, returning a hashref of
	#	the first available response(s), keyed by id
	#
	my $result = $q->wait_any(@ids);
	#
	#	wait upto $timeout seconds for a response to 
	#	the queued elements listed in @ids, returning 
	#	a hashref of the first available response(s), keyed by id
	#	Returns undef if none available in $timeout seconds
	#
	my $result = $q->wait_any_until($timeout, @ids);
	#
	#	wait for responses to all the queued 
	#	elements listed in @ids, returning a hashref of
	#	the response(s), keyed by id
	#
	my $result = $q->wait_all(@ids);
	#
	#	wait upto $timeout seconds for responses to 
	#	all the queued elements listed in @ids, returning 
	#	a hashref of the response(s), keyed by id
	#	Returns undef if all responses not recv'd 
	#	in $timeout seconds
	#
	my $result = $q->wait_all_until($timeout, @ids);
	#
	#	mark an existing request
	#
	$q->mark($id, 'CANCEL');
	#
	#	test if a request is marked
	#
	print "Marked for cancel!"
		if $q->marked($id, 'CANCEL');
	#
	#	cancel specific operations
	#
	my $result = $q->cancel(@ids);
	#
	#	cancel all operations
	#
	my $result = $q->cancel_all();
	#
	#	test if specified request has been cancelled
	#
	my $result = $q->cancelled($id);
	#
	#	(class-level method) wait for an event on
	#	any of the listed queue objects. Returns a
	#	list of queues which have events pending
	#
	my $result = Thread::Queue::Duplex->wait_any( 
		[ $q1 ], [ $q2, @ids ]);
	#
	#	(class-level method) wait upto $timeout seconds 
	#	for an event on any of the listed queue objects.
	#	Returns undef if none available in $timeout seconds,
	#	otherwise, returns a list of queues with events pending
	#
	my $result = Thread::Queue::Duplex->wait_any_until(
		$timeout, [ $q1 ], [ $q2, @ids ]);
	#
	#	(class-level method) wait for events on all the listed
	#	queue objects. Returns the list of queue objects.
	#
	my $result = Thread::Queue::Duplex->wait_all( 
		[ $q1 ], [ $q2, @ids ]);
	#
	#	(class-level method) wait upto $timeout seconds for 
	#	events on all the listed queue objects.
	#	Returns empty list if all listed queues do not have
	#	an event in $timeout seconds, otherwise returns
	#	the list of queues
	#
	my $result = Thread::Queue::Duplex->wait_all_until(
		$timeout, [ $q1 ], [ $q2, @ids ]);

=head1 DESCRIPTION

A mapped queue, similar to L<Thread::Queue>, except that as elements
are queued, they are assigned unique identifiers, which are used
to identify responses returned from the dequeueing thread. This
class provides a simple RPC-like mechanism between multiple client
and server threads, so that a single server thread can safely
multiplex requests from multiple client threads. B<Note> that
simplex versions of the enqueue methods are provided which
do not assign unique identifiers, and are used for requests
to which no response is required/expected.

In addition, elements are inspected as they are enqueued/dequeued to determine
if they are L<Thread::Queue::Queueable> (I<aka TQQ>) objects, and, if so, 
the onEnqueue() or onDequeue() methods are called to permit any 
additional class-specific marshalling/unmarshalling to be performed. 
B<NOTE:> Thread::Queue::Duplex (I<aka TQD>) is itself a 
L<Thread::Queue::Queueable> object, thus permitting TQD
objects to be passed between threads.

Various C<wait()> methods are provided to permit waiting on individual
responses, any or all of a list of responses, and time-limited waits
for each. Additionally, class-level versions of the C<wait()> methods
are provided to permit a thread to simultaneously wait for either
enqueue or response events on any of a number of queues.

A C<mark()> method is provided to permit out-of-band information
to be applied to pending requests. A responder may test for marks
via the C<marked()> method prior to C<respond()>ing to a request.
An application may specify a mark value, which the responder can
test for; if no explicit mark value is given, the value 1 is used.

C<cancel()> and C<cancel_all()> methods are provided to
explicitly cancel one or more requests, and invoke the
C<onCancel()> method of any L<Thread::Queue::Queueable> objects
in the request. Cancelling will result in one of

=over 4

=item marking the request as cancelled if
it has not yet been dequeued (note that it cannot be 
spliced from the queue due C<threads::shared>'s lack 
of support for array splicing)

=item removal and discarding of the response from the response map
if the request has already been processed

=item if the request is in progress, the responder will
detect the cancellation when it attempts to C<respond()>,
and the response will be discarded

=back

C<listen()> and C<ignore()> methods are provided so that
server-side threads can register/unregister as listeners
on the queue; the constructor accepts a "ListenerRequired"
attribute argument. If set, then any C<enqueue()>
operation will fail and return undef if there are no
registered listeners. This feature provides some safeguard
against "stuck" requestor threads when the responder(s)
have shutdown for some reason. In addition, a C<wait_for_listener()>
method is provided to permit an initiating thread to wait
until another thread registers as a listener.

The constructor also accepts a C<MaxPending> attribute
that specifies the maximum number of requests that may
be pending in the queue before the operation will block.
Note that responses are not counted in this limit.

C<Thread::Queue::Duplex> objects encapsulate 

=over 4

=item a shared array, used as the queue (same as L<Thread::Queue>)

=item a shared scalar, used to provide unique identifier sequence
numbers

=item a shared hash, I<aka> the mapping hash, used to return responses 
to enqueued elements, using the generated uniqiue identifier as the hash key

=item a listener count, incremented each time C<listen()> is called,
decremented each time C<ignore()> is called, and, if
the "listener required" flag has been set on construction, tested
for each C<enqueue()> call.

=back

A normal processing sequence for Thread::Queue::Duplex might be:

	#
	#	Thread A (the client):
	#
		...marshal parameters for a coroutine...
		my $id = $q->enqueue('function_name', \@paramlist);
		my $results = $q->dequeue_response($id);
		...process $results...
	#
	#	Thread B (the server):
	#
		while (1) {
			my $call = $q->dequeue;
			my ($id, $func, @params) = @$call;
			$q->respond($id, $self->$func(@params));
		}

=head1 FUNCTIONS AND METHODS

=over 8

=item new([ListenerRequired => $val, MaxPending => $limit])

The C<new> function creates a new empty queue,
and associated mapping hash. If a "true" C<ListenerRequired>
value is provided, then all enqueue operations require that
at least one thread has registered as a listener via C<listen()>.
If the C<MaxPending> value is a non-zero value, the number
of pending requests will be limited to C<$limit>, and any further
attempt to queue a request will block until the pending count
drops below C<$limit>. This limit may be applied or modified later
via the C<set_max_pending()> method (see below).

=item enqueue(@request)

Creates a shared array, pushes a unique
identifier onto the shared array, then pushes the LIST onto the array,
then pushes the shared arrayref onto the queue.

=item enqueue_urgent(@request)

Same as L<enqueue>, but adds the element to head of queue, rather
than tail.

=item enqueue_simplex(@request)

Same as L<enqueue>, but does not allocate an identifier, nor
expect a response.

=item enqueue_simplex_urgent(@request)

Same as L<enqueue_simplex>, but adds the element to head of queue,
rather than tail.

=item dequeue

Waits indefinitely for an element to become available
in the queue, then removes and returns it.

=item dequeue_nb

The C<dequeue_nb> method is identical to C<dequeue>(),
except it will return undef immediately if there are no
elements currently in the queue.

=item dequeue_until

Identical to C<dequeue>(), except it accepts a C<$timeout> 
parameter specifying a duration (in seconds) to wait for 
an available element. If no element is
available within the $timeout, it returns undef.

=item dequeue_urgent

Identical to C<dequeue_nb>(), except it only
returns the next available element if it has been queued 
via either C<enqueue_urgent>() or C<enqueue_simplex_urgent>().
Useful for servers which poll for events, e.g.,
for external aborts of long-running operations.

=item pending

Returns the number of items still in the queue.

=item set_max_pending($limit)

Set the maximum number of requests that may be queued
without blocking the requestor.

=item respond($id [, LIST ])

Creates a new element in the mapping hash, keyed by C<$id>,
with a value set to a shared arrayref containing LIST.
If C<$id> is C<undef>, the operation is silently ignored
(in order to gracefully support simplex requests).

=item ready($id)

Tests for a response to a uniquely identified 
previously C<enqueue>'d LIST. Returns undef if no
response is available, otherwise returns the 
Thread::Queue::Duplex object.

=item available([@ids])

Returns list of available response ID's;
if C<@ids> provided, only returns ID's from the list.
Returns undef if none available; in scalar context, 
returns only first available; else a returns a 
list of available IDs.

=item wait($id) I<aka> dequeue_response($id)

Waits indefinitely for a response to a uniquely identified 
previously C<enqueue>'d LIST. Returns the returned result.

=item wait_until($id, $timeout)

Waits up to $timeout seconds for a response to 
to a uniquely identified previously C<enqueue>'d LIST.
Returns undef if no response is available in the specified
$timeout duration, otherwise, returns the result.

=item wait_any(@ids) I<[instance method form]>

Wait indefinitely for a response to any of the 
previously C<enqueue>'d elements specified in the
the supplied C<@ids>. Returns a hashref of available 
responses keyed by their identifiers

=item wait_any_until($timeout, @ids) I<[instance method form]>

Wait upto $timeout seconds for a response to any of the 
previously C<enqueue>'d elements specified in the
the supplied C<@ids>. Returns a hashref of available 
responses keyed by their identifiers, or undef if none
available within $timeout seconds.

=item wait_all(@ids) I<[instance method form]>

Wait indefinitely for a response to all the 
previously C<enqueue>'d elements specified in
the supplied C<@ids>. Returns a hashref of
responses keyed by their identifiers.

=item wait_all_until($timeout, @ids) I<[instance method form]>

Wait upto C<$timeout> seconds for a response to all the 
previously C<enqueue>'d elements specified in
the supplied C<@ids>. Returns a hashref of
responses keyed by their identifiers, or undef if all
responses are not available within $timeout seconds.

=item wait_any(@queue_refs) I<[class method form]>

Wait indefinitely for an event on any of the listed queue objects. 
Returns a list of queues which have events pending. C<@queue_refs>
elements may be either TQD objects, or arrayrefs whose first element 
is a TQD object, and the remaining elements are queue'd element 
identifiers. For bare TQD elements and arrayref elements
with no identifiers, C<wait_any> waits for an enqueue event
on the queue; otherwise, it waits for a response event for any
of the specified identifiers.

B<NOTE:>In order for the class method form of these C<wait_XXX()>
functions to behave properly, the "main" application should
"C<use Thread::Queue::Duplex;>" in order to install the
class-level shared variable used for signalling events
across all TQD instances. Failure to do so could cause a segregation
of TQD objects created in threads descended from different
parent threads (due to the perl interpretter cloning 
when threads are created).

B<Also note> that only enqueue and response events are detected;
cancel events are not reported by these class methods.

B<Finally>, note that there is no guarantee that the queue objects
returned by the class-level C<wait()> methods will still have events
pending on them when they are returned, since multiple threads
may be notified of an event on the same queue, but one
thread may have handled the event before the other thread(s).

=item wait_any_until($timeout, @queue_refs) I<[class method form]>

Wait upto C<$timeout> seconds for an event on any of the listed 
queue objects. Returns undef if none available in C<$timeout> seconds,
otherwise, returns a list of queues with events pending. C<@queue_refs>
is the same as for C<wait_any()>.

=item wait_all(@queue_refs) I<[class method form]>

Wait indefinitely for events on all the listed
queue objects. Returns the list of queue objects.

=item wait_all_until($timeout, @queue_refs) I<[class method form]>

Wait upto C<$timeout> seconds for events on all the listed 
queue objects. Returns an empty list if all listed queues do not have
an event in C<$timeout> seconds, otherwise returns
the list of queue objects.

=item C<mark($id [ , $value ])>

Marks the specified request with the given value.
If the request has not been processed, it will be marked
with the specified C<$value>; if no C<$value> is given,
the mark is set to 1. The request may later be tested
for a given mark via the C<marked($id [, $value ])>
method. This is useful for out-of-band tagging of
requests, e.g., to cancel a request but retaining
a response for the request.

Note that C<$value>' are not cumulative, i.e., if multiple
marks are applied, only the most recent C<$value> is retained.

=item C<marked($id [, $value ])>

Tests if the specified request has been marked with
the given C<$value>. If no C<$value> is specified, simply tests
if the request is marked. Returns true or false.

=item C<get_mark($id)>

Returns the current mark value of a request, if any.

=item cancel(@ids)

Cancels all the requests identified in C<@ids>. 
If a response to a cancelled request has already been
posted to the queue response map (i.e., the request has already 
been serviced), the response is removed from the map, 
the C<onCancel()> method is invoked on each 
L<Thread::Queue::Queueable> object in the response,
and the response is discarded.

If a response to a cancelled request has B<not> yet been posted to 
the queue response map, an empty entry is added to the queue response map. 
(B<Note>: L<threads::shared> doesn't permit splicing shared arrays yet,
so we can't remove the request from the queue).

When a server thread attempts to C<dequeue[_nb|_until]()> a cancelled 
request, the request is discarded and the dequeue operation is retried. 
If the cancelled request is already dequeued, the server thread will 
detect the cancellation when it attempts to C<respond()> to the request,
and will invoke the C<onCancel()> method on any L<Thread::Queue::Queueable>
objects in the response, and then discards the response.

B<Note> that, simplex requests do not have an identifier, there
is no way to explicitly cancel a specific simplex request.

=item cancel_all()

Cancels B<all> current requests and responses, using the C<cancel()>
algorithm above, plus cancels all simplex requests still
in the queue.

B<Note:> In-progress requests (i.e.,
request which have been removed from the queue, but do not yet
have an entry in the response map)  will B<not> be cancelled.

=back

=head1 SEE ALSO

L<Thread::Queue::Queueable>, L<threads>, L<threads::shared>, L<Thread::Queue>

=head1 AUTHOR, COPYRIGHT, & LICENSE

Dean Arnold, Presicient Corp. L<darnold@presicient.com>

Copyright(C) 2005, Presicient Corp., USA

Permission is granted to use this software under the same terms
as Perl itself. Refer to the Perl Artistic License for details.

=cut

#
#	global semaphore used for class-level wait()
#	notification
#
our $tqd_global_lock : shared = 0;

use constant TQD_Q => 0;
use constant TQD_MAP => 1;
use constant TQD_IDGEN => 2;
use constant TQD_LISTENERS => 3;
use constant TQD_REQUIRE_LISTENER => 4;
use constant TQD_MAX_PENDING => 5;
use constant TQD_URGENT_COUNT => 6;
use constant TQD_MARKS => 7;

sub new {
    my $class = shift;
    
    $@ = 'Invalid argument list',
    return undef
    	if (scalar @_ && (scalar @_ & 1));
    
    my %args = @_;
    foreach (keys %args) {
	    $@ = 'Invalid argument list',
	    return undef
	    	unless ($_ eq 'ListenerRequired') ||
	    		($_ eq 'MaxPending');

	    $@ = 'Invalid argument list',
	    return undef
	    	if (($_ eq 'MaxPending') && 
	    		(($args{$_}!~/^\d+/) || ($args{$_} < 0)));
    }
    my $idgen : shared = 1;
    my $listeners : shared = 0;
	my $max_pending : shared = $args{MaxPending};
	my $urgent_count : shared = 0;
	my %marks : shared = ();
    my $obj = [
    	&share([]),
    	&share({}),
    	\$idgen,
    	\$listeners,
    	$args{ListenerRequired},
    	\$max_pending,
    	\$urgent_count,
    	\%marks
    ];
    	
    return bless $obj, $class;
}

sub listen {
	my $obj = shift;
	lock(${$obj->[TQD_LISTENERS]});
	${$obj->[TQD_LISTENERS]}++;
	cond_broadcast(${$obj->[TQD_LISTENERS]});
	return $obj;
}

sub ignore {
	my $obj = shift;
	lock(${$obj->[TQD_LISTENERS]});
	${$obj->[TQD_LISTENERS]}--
		if ${$obj->[TQD_LISTENERS]};
	return $obj;
}
#
#	wait until we've got a listener
#
sub wait_for_listener {
	my ($obj, $timeout) = shift;
	lock(${$obj->[TQD_LISTENERS]});

	return undef 
		if ($timeout && ($timeout < 0));

	if ($timeout) {
		$timeout += time();
		1 while ((!${$obj->[TQD_LISTENERS]}) &&
			cond_timedwait(${$obj->[TQD_LISTENERS]}, $timeout));
		return ${$obj->[TQD_LISTENERS]} ? $obj : undef;
	}
	1 while ((!${$obj->[TQD_LISTENERS]}) &&
		cond_wait(${$obj->[TQD_LISTENERS]}));
	return ${$obj->[TQD_LISTENERS]} ? $obj : undef;
}
#
#	common function for build enqueue list
#
sub _filter_nq {
	my $id = shift;
	my @params : shared = ($id, (undef) x (scalar @_ << 1));
#
#	marshall params, checking for Queueable objects
#
	my $i = 1;
	foreach (@_) {
		if (ref $_ && 
			(ref $_ ne 'ARRAY') &&
			(ref $_ ne 'HASH') &&
			(ref $_ ne 'SCALAR') &&
			$_->isa('Thread::Queue::Queueable')) {
#
#	invoke onEnqueue method
#
			$params[$i] = ref $_;
			$params[$i+1] = $_->onEnqueue();
		}
		else {
			@params[$i..$i+1] = (undef, $_);
		}
		$i += 2;
	}
	return \@params;
}

sub _lock_load {
	my ($obj, $params, $urgent) = @_;
	lock(@{$obj->[TQD_Q]});
#
#	check current length if we have a limit
#
	while (${$obj->[TQD_MAX_PENDING]} &&
		(${$obj->[TQD_MAX_PENDING]} <= scalar @{$obj->[TQD_Q]})) {
#		print "pending before: ", scalar @{$obj->[TQD_Q]}, "\n";
		cond_wait(@{$obj->[TQD_Q]});
#		print "pending after: ", scalar @{$obj->[TQD_Q]}, "\n";
	}

	if ($urgent) {
	    unshift @{$obj->[TQD_Q]}, $params;
	    ${$obj->[TQD_URGENT_COUNT]}++;
	}
	else {
	    push @{$obj->[TQD_Q]}, $params;
	}
    cond_signal @{$obj->[TQD_Q]};
    1;
}

sub _get_id {
	my $obj = shift;
	
	lock(${$obj->[TQD_IDGEN]});
	my $id = ${$obj->[TQD_IDGEN]}++;
#
#	rollover, just in case...not perfect,
#	but good enough
#
	${$obj->[TQD_IDGEN]} = 1
		if (${$obj->[TQD_IDGEN]} > 2147483647);
	return $id;
}

sub enqueue {
    my $obj = shift;

	return undef 
		if ($obj->[TQD_REQUIRE_LISTENER] &&
			(! ${$obj->[TQD_LISTENERS]}));
	my $id = $obj->_get_id;

	my $params = _filter_nq($id, @_);
	_lock_load($obj, $params);
	lock($tqd_global_lock);
	cond_broadcast($tqd_global_lock);
    return $id;
}

sub enqueue_urgent {
    my $obj = shift;

	return undef 
		if ($obj->[TQD_REQUIRE_LISTENER] &&
			(! ${$obj->[TQD_LISTENERS]}));
	my $id = $obj->_get_id;
	my $params = _filter_nq($id, @_);
	_lock_load($obj, $params, 1);

	lock($tqd_global_lock);
	cond_broadcast($tqd_global_lock);
    return $id;
}
#
#	Simplex versions
#
sub enqueue_simplex {
    my $obj = shift;

	return undef 
		if ($obj->[TQD_REQUIRE_LISTENER] &&
			(! ${$obj->[TQD_LISTENERS]}));

	my $params = _filter_nq(undef, @_);
	_lock_load($obj, $params);

	lock($tqd_global_lock);
	cond_broadcast($tqd_global_lock);
    return $obj;
}

sub enqueue_simplex_urgent {
    my $obj = shift;

	return undef 
		if ($obj->[TQD_REQUIRE_LISTENER] &&
			(! ${$obj->[TQD_LISTENERS]}));

	my $params = _filter_nq(undef, @_);
	_lock_load($obj, $params, 1);

	lock($tqd_global_lock);
	cond_broadcast($tqd_global_lock);
    return $obj;
}
#
#	recover original param list, including reblessing Queueables
#
sub _filter_dq {
	my $result = shift;
#
#	keep ID; collapse the rest
#
	my @results = (shift @$result);
	my $class;
	$class = shift @$result,
	push (@results,
		$class ? 
		${class}->onDequeue(shift @$result) :
		shift @$result)
		while (@$result);
    return \@results;
}

sub dequeue  {
    my $obj = shift;
    my $request;
    while (1) {
#
#	lock order is important here
#
		lock(@{$obj->[TQD_Q]});
    	cond_wait @{$obj->[TQD_Q]}
    		until scalar @{$obj->[TQD_Q]};
    	$request = shift @{$obj->[TQD_Q]};
#   print threads->self()->tid(), " dequeue\n";
    	${$obj->[TQD_URGENT_COUNT]}--
    		if ${$obj->[TQD_URGENT_COUNT]};
#
#	cancelled request ?
#
   		next
   			if ($request->[0] && ($request->[0] == -1));
#
#	check for cancel
#
		{
			lock(%{$obj->[TQD_MAP]});
			delete $obj->[TQD_MAP]{$request->[0]},
			next
				if ($request->[0] &&
					exists $obj->[TQD_MAP]{$request->[0]});
		}
#
#	signal any waiters
#
    	cond_broadcast @{$obj->[TQD_Q]};
    	last;
    }
    return _filter_dq($request);
}

sub dequeue_until {
    my ($obj, $timeout) = @_;

    return undef 
    	unless $timeout && ($timeout > 0);

	$timeout += time();
	my $request;

	while (1)
	{
		lock(@{$obj->[TQD_Q]});
	
		1
	   	while ((! scalar @{$obj->[TQD_Q]}) &&
	   		cond_timedwait(@{$obj->[TQD_Q]}, $timeout));
#
#	if none, then we must've timed out
#
		return undef
			unless scalar @{$obj->[TQD_Q]};

		$request = shift @{$obj->[TQD_Q]};
#   print threads->self()->tid(), " dequeue_until\n";
    	${$obj->[TQD_URGENT_COUNT]}--
    		if ${$obj->[TQD_URGENT_COUNT]};
#
#	cancelled request ?
#
   		next
   			if ($request->[0] && ($request->[0] == -1));
#
#	check for cancel
#
		{
			lock(%{$obj->[TQD_MAP]});
			delete $obj->[TQD_MAP]{$request->[0]},
			next
				if ($request->[0] &&
					exists $obj->[TQD_MAP]{$request->[0]});
		}
#
#	signal any waiters
#
    	cond_broadcast @{$obj->[TQD_Q]};
    	last;
	}
    return _filter_dq($request);
}

sub dequeue_nb {
    my $obj = shift;
    my $request;
    while (1)
    {
		lock(@{$obj->[TQD_Q]});
		return undef 
			unless scalar @{$obj->[TQD_Q]};
		$request = shift @{$obj->[TQD_Q]};
#   print threads->self()->tid(), " dequeue_nb\n";
    	${$obj->[TQD_URGENT_COUNT]}--
    		if ${$obj->[TQD_URGENT_COUNT]};
#
#	cancelled request ?
#
   		next
   			if ($request->[0] && ($request->[0] == -1));
#
#	check for cancel (ie, the request is already in the map)
#
		{
			lock(%{$obj->[TQD_MAP]});
			delete $obj->[TQD_MAP]{$request->[0]},
			next
				if ($request->[0] &&
					exists $obj->[TQD_MAP]{$request->[0]});
		}
#
#	signal any waiters
#
    	cond_broadcast @{$obj->[TQD_Q]};
		last;
	}
    return _filter_dq($request);
}

sub dequeue_urgent {
    my $obj = shift;
    my $request;
    while (1)
    {
		lock(@{$obj->[TQD_Q]});
		return undef 
			unless (scalar @{$obj->[TQD_Q]}) && 
				${$obj->[TQD_URGENT_COUNT]};
		$request = shift @{$obj->[TQD_Q]};
    	${$obj->[TQD_URGENT_COUNT]}--
    		if ${$obj->[TQD_URGENT_COUNT]};
#
#	cancelled request ?
#
   		next
   			if ($request->[0] && ($request->[0] == -1));
#
#	check for cancel
#
		{
			lock(%{$obj->[TQD_MAP]});
			delete $obj->[TQD_MAP]{$request->[0]},
			next
				if ($request->[0] &&
					exists $obj->[TQD_MAP]{$request->[0]});
		}
#
#	signal any waiters
#
    	cond_broadcast @{$obj->[TQD_Q]};
    	last;
	}
    return _filter_dq($request);
}

sub pending {
    my $obj = shift;
	lock(@{$obj->[TQD_Q]});
	lock(%{$obj->[TQD_MAP]});
	my $p = scalar @{$obj->[TQD_Q]};
	my $i;
	foreach (@{$obj->[TQD_Q]}) {
		next unless $_ && ref $_ && (ref $_ eq 'ARRAY') && $_->[0];
#
#	NOTE: this intermediate assignment is required for no apparent
#	reason in order to keep from getting "Free to wrong pool" aborts
#
		$i = $_->[0];
		$p--
			if ($i == -1) || exists($obj->[TQD_MAP]{$i});
	}
	return $p;
}

sub set_max_pending {
    my ($obj, $limit) = @_;
    $@ = 'Invalid limit.',
    return undef
    	unless (defined($limit) && ($limit=~/^\d+/) && ($limit >= 0));
  
	lock(@{$obj->[TQD_Q]});
    ${$obj->[TQD_MAX_PENDING]} = $limit;
#
#	wake up anyone whos been waiting for queue to change
#
	cond_broadcast(@{$obj->[TQD_Q]});
    return $obj;
}

#
#	common function for building response list
#
sub _create_resp {
	my @params : shared = ((undef) x (scalar @_ << 1));
#
#	marshall params, checking for Queueable objects
#
	my $i = 0;
	foreach (@_) {
		if (ref $_ && 
			(ref $_ ne 'ARRAY') &&
			(ref $_ ne 'HASH') &&
			(ref $_ ne 'SCALAR') &&
			$_->isa('Thread::Queue::Queueable')) {
#
#	invoke onEnqueue method
#
			$params[$i] = ref $_;
			$params[$i+1] = $_->onEnqueue();
		}
		else {
			@params[$i..$i+1] = (undef, $_);
		}
		$i += 2;
	}
	return \@params;
}

sub respond {
	my $obj = shift;
	my $id = shift;
#
#	silently ignore response to a simplex request
#
	return $obj unless defined($id);

	my $result = _create_resp(@_);
	{
		lock(%{$obj->[TQD_MARKS]});
		delete $obj->[TQD_MARKS]{$id};

		lock(%{$obj->[TQD_MAP]});
#
#	check if its been canceled
#
		_cancel_resp($result),
		return $obj
			if exists $obj->[TQD_MAP]{$id};

		$obj->[TQD_MAP]{$id} = $result;
	    cond_signal %{$obj->[TQD_MAP]};
	}
	lock($tqd_global_lock);
	cond_broadcast($tqd_global_lock);

	return $obj;
}
#
#	common function for filtering response list
#
sub _filter_resp {
	my $result = shift;
#
#	collapse the response elements
#
	my @results = ();
	my $class;
	$class = shift @$result,
	push (@results,
		$class ? 
		${class}->onDequeue(shift @$result) :
		shift @$result)
		while (@$result);
    return \@results;
}

sub wait {
	my $obj = shift;
	my $id = shift;	

	my $result;
	{
		lock(%{$obj->[TQD_MAP]});
		unless ($obj->[TQD_MAP]{$id}) {
		    cond_wait %{$obj->[TQD_MAP]}
		    	until $obj->[TQD_MAP]{$id};
		}
    	$result = delete $obj->[TQD_MAP]{$id};
    	cond_signal %{$obj->[TQD_MAP]}
    		if keys %{$obj->[TQD_MAP]};
    }
    return _filter_resp($result);
}
*dequeue_response = \&wait;

sub ready {
	my $obj = shift;
	my $id = shift;
#
#	no lock really needed here...
#
    return defined($obj->[TQD_MAP]{$id}) ? $obj : undef;
}

sub available {
	my $obj = shift;

	my @ids = ();
	lock(%{$obj->[TQD_MAP]});
	if (scalar @_) {
		map { push @ids, $_ if $obj->[TQD_MAP]{$_}; } @_;
	}
	else {
		@ids = keys %{$obj->[TQD_MAP]};
	}
    return scalar @ids ? wantarray ? @ids : $ids[0] : undef;
}

sub wait_until {
	my ($obj, $id, $timeout) = @_;

    return undef 
    	unless $timeout && ($timeout > 0);
	$timeout += time();
	my $result;

	{
		lock(%{$obj->[TQD_MAP]});
		1
	    	while ((! $obj->[TQD_MAP]{$id}) &&
	    		cond_timedwait(%{$obj->[TQD_MAP]}, $timeout));

	   	$result = delete $obj->[TQD_MAP]{$id}
	   		if $obj->[TQD_MAP]{$id};

	    cond_signal %{$obj->[TQD_MAP]}
	    	if keys %{$obj->[TQD_MAP]};
	}
    return $result ? _filter_resp($result) : undef;
}
#
#	some grouped waits
#	wait indefinitely for *any* of the
#	supplied ids
#
sub wait_any {
	my $obj = shift;
	return _tqd_wait(undef, undef, @_)
		unless ref $obj;

	my %responses = ();
	{
		lock(%{$obj->[TQD_MAP]});
#
#	cond_wait isn't behaving as expected, so we need to 
#	test first, then wait if needed
#
   		map { 
   			$responses{$_} = delete $obj->[TQD_MAP]{$_}
   				if $obj->[TQD_MAP]{$_}; 
   		} @_;

		until (keys %responses) {
			cond_wait %{$obj->[TQD_MAP]};
		   	map { 
		   		$responses{$_} = delete $obj->[TQD_MAP]{$_}
		   			if $obj->[TQD_MAP]{$_}; 
		   	} @_;
#
#	go ahead and signal...if no one's waiting, no harm
#
		    cond_signal %{$obj->[TQD_MAP]};
		}
	}
	$responses{$_} = _filter_resp($responses{$_})
		foreach (keys %responses);
    return \%responses;
}
#
#	wait up to timeout for any
#
sub wait_any_until {
	my $obj = shift;
	return _tqd_wait(shift, undef, @_)
		unless ref $obj;

	my $timeout = shift;
	
	return undef unless $timeout && ($timeout > 0);
	$timeout += time();
	
	my %responses = ();
	{
		lock(%{$obj->[TQD_MAP]});
#
#	cond_wait isn't behaving as expected, so we need to 
#	test first, then wait if needed
#
	   	map { 
	   		$responses{$_} = delete $obj->[TQD_MAP]{$_}
	   			if $obj->[TQD_MAP]{$_}; 
	   	} @_;

		while ((! keys %responses) && 
			cond_timedwait(%{$obj->[TQD_MAP]}, $timeout)) {

	   		map { 
		   		$responses{$_} = delete $obj->[TQD_MAP]{$_}
	   				if $obj->[TQD_MAP]{$_}; 
	   		} @_;
#
#	go ahead and signal...if no one's waiting, no harm
#
		    cond_signal %{$obj->[TQD_MAP]};
		}
	}
	$responses{$_} = _filter_resp($responses{$_})
		foreach (keys %responses);
    return keys %responses ? \%responses : undef;
}

sub wait_all {
	my $obj = shift;
	return _tqd_wait(undef, 1, @_)
		unless ref $obj;

	my %responses = ();
	{
		lock(%{$obj->[TQD_MAP]});
	   	map { 
	   		$responses{$_} = delete $obj->[TQD_MAP]{$_}
	   			if $obj->[TQD_MAP]{$_}; 
	   	} @_;
		until (scalar keys %responses == scalar @_) {
			cond_wait %{$obj->[TQD_MAP]};

		   	map { 
		   		$responses{$_} = delete $obj->[TQD_MAP]{$_}
		   			if $obj->[TQD_MAP]{$_}; 
		   	} @_;
#
#	go ahead and signal...if no one's waiting, no harm
#
		    cond_signal %{$obj->[TQD_MAP]};
		}
	}
	$responses{$_} = _filter_resp($responses{$_})
		foreach (keys %responses);
    return \%responses;
}

sub wait_all_until {
	my $obj = shift;

	return _tqd_wait(shift, 1, @_)
		unless ref $obj;

	my $timeout = shift;
	
	return undef unless $timeout && ($timeout > 0);
	$timeout += time();
	
	my %responses = ();
	{
		lock(%{$obj->[TQD_MAP]});
   		map { 
   			$responses{$_} = delete $obj->[TQD_MAP]{$_}
   				if $obj->[TQD_MAP]{$_}; 
   		} @_;
		while ((scalar keys %responses != scalar @_) && 
			cond_timedwait(%{$obj->[TQD_MAP]}, $timeout)) {

   			map {
		   		$responses{$_} = $obj->[TQD_MAP]{$_}
   					if $obj->[TQD_MAP]{$_}; 
   			} @_;
#
#	go ahead and signal...if no one's waiting, no harm
#	
		    cond_signal %{$obj->[TQD_MAP]};
		}
#
#	if we got all our responses, then remove from map
#
		map { delete $obj->[TQD_MAP]{$_} } @_
			if (scalar keys %responses == scalar @_);
	}
	$responses{$_} = _filter_resp($responses{$_})
		foreach (keys %responses);
#print 'list has ', scalar @_, ' we got ', scalar keys %responses, "\n";
    return (scalar keys %responses == scalar @_) ? \%responses : undef;
}

sub mark {
	my ($obj, $id, $value) = @_;
	
	$value = 1 unless defined($value);
	lock(%{$obj->[TQD_MAP]});
	lock(%{$obj->[TQD_MARKS]});
#
#	already responded or cancelled
#
	return undef
		if (exists $obj->[TQD_MAP]{$id});

	$obj->[TQD_MARKS]{$id} = $value;
	return $obj;
}

sub unmark {
	my ($obj, $id) = @_;
	
	lock(%{$obj->[TQD_MARKS]});
	delete $obj->[TQD_MARKS]{$id};
	return $obj;
}

sub get_mark {
	my ($obj, $id) = @_;
	lock(%{$obj->[TQD_MARKS]});
	return $obj->[TQD_MARKS]{$id};
}

sub marked {
	my ($obj, $id, $value) = @_;
	lock(%{$obj->[TQD_MARKS]});
	return (defined($obj->[TQD_MARKS]{$id}) &&
		((defined($value) && ($obj->[TQD_MARKS]{$id} eq $value)) ||
		(!defined($value))));
}

sub _cancel_resp {
	my $resp = shift;
#
#	collapse the response elements,
#	and call onCancel to any that are Queueables
#
	my $class;
	$class = shift @$resp,
	$class ?
		${class}->onCancel(shift @$resp) :
		shift @$resp
		while (@$resp);
    return 1;
}

sub cancel {
	my $obj = shift;
#
#	when we lock both, *always* lock in this order to avoid
#	deadlock
#
	lock(@{$obj->[TQD_Q]});
	lock(%{$obj->[TQD_MAP]});
	lock(%{$obj->[TQD_MARKS]});
	foreach (@_) {
		delete $obj->[TQD_MARKS]{$_};

		$obj->[TQD_MAP]{$_} = undef,
		next
			unless (exists $obj->[TQD_MAP]{$_});
#
#	already responded to, call onCancel for any Queueuables
#
		_cancel_resp(delete $obj->[TQD_MAP]{$_});
	}
	return $obj;
}

sub cancel_all {
	my $obj = shift;
#
#	when we lock both, *always* lock in this order to avoid
#	deadlock
#
	lock(@{$obj->[TQD_Q]});
	lock(%{$obj->[TQD_MAP]});
	lock(%{$obj->[TQD_MARKS]});
#
#	first cancel all pending responses
#
	_cancel_resp(delete $obj->[TQD_MAP]{$_})
		foreach (keys %{$obj->[TQD_MAP]});
#
#	then cancel all the pending requests by
#	setting their IDs to -1
#
	delete $obj->[TQD_MARKS]{$_->[0]},
	$_->[0] = -1
		foreach (@{$obj->[TQD_Q]});
#
#	how will we cancel inprogress requests ??
#	need a map value, or alternate map...
#
	return $obj;
}
##########################################################
#
#	BEGIN CLASS LEVEL METHODS
#
##########################################################

sub _tqd_wait {
	my $timeout = shift;
	my $wait_all = shift;
#
#	validate params
#
	map {
		return undef 
			unless ($_ && 
				ref $_ && 
				(
					((ref $_ eq 'ARRAY') && 
						($#$_ >= 0) &&
						ref $_->[0] &&
						(ref $_->[0] eq 'Thread::Queue::Duplex')) ||
					$_->[0]->isa('Thread::Queue::Duplex')
				));
	} @_;

	my @remain = ();
	my @avail = ();

	$timeout += time() if $timeout;

	while (1) {
		lock($tqd_global_lock);
		foreach (@_) {
			my $q = (ref $_ eq 'ARRAY') ? $_->[0] : $_;
			if ((ref $_ ne 'ARRAY') ||
				((ref $_ eq 'ARRAY') && (scalar @$_ == 1))) {
#
#	queue only, check for pending requests
#
				lock(@{$q->[TQD_Q]});
		    	(scalar @{$q->[TQD_Q]}) ?
					push @avail, $q :
					push @remain, $_;
				next;
			}
#
#	we've got ids, so check for responses
#
			my @ids = @$_;
			shift @ids;
			($q->available(@ids)) ?
				push @avail, $q :
				push @remain, $_;
		}	# end foreach queue
		last
			unless (($wait_all && scalar @remain) || (! scalar @avail));
#
#	reset wait list
#
		@_ = @remain;
		@remain = ();

		cond_wait($tqd_global_lock),
		next
			unless $timeout;

		return ()
			unless cond_timedwait($tqd_global_lock, $timeout);
	}

	return @avail;
}
##########################################################
#
#	END CLASS LEVEL METHODS
#
##########################################################

###############################################
#	
#	OVERRIDE TQQ METHODS
#	default onEnqueue, onDequeue, and onCancel OK
#
###############################################
#
#	provide curse and redeem methods
#	to support passing between threads
#
sub curse {
	my $obj = shift;
#
#	NOTE: yes we have to init separately from declaration;
#	it seems shared doesn't copy things properly
#
	my @tqd : shared = ();
	$tqd[$_] = $obj->[$_] foreach (0..5);
	return \@tqd;
}

sub redeem {
	my ($class, $obj) = @_;
	return bless [ @$obj ], $class;
}

1;
