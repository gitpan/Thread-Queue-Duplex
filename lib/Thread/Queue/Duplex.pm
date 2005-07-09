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

use strict;
use warnings;

our $VERSION = '0.10';

=head1 NAME

Thread::Queue::Duplex - thread-safe request/response queue with identifiable elements

=head1 SYNOPSIS

	use Thread::Queue::Duplex;
	my $q = new Thread::Queue::Duplex;
	#
	#	enqueue elements, returning a unique queue ID
	#	(used in the client)
	#
	my $id = $q->enqueue("foo", "bar");
	#
	#	dequeue next available element (used in the server),
	#	waiting indefinitely for an element to be made available
	#	returns shared arrayref, first element is unique ID
	#
	my $foo = $q->dequeue;
	#
	#	dequeue next available element (used in the server),
	#	returns undef if no element immediately available
	#	otherwise, returns shared arrayref, first element is unique ID
	#
	my $foo = $q->dequeue_nb;
	#
	#	dequeue next available element (used in the server),
	#	returned undef if no element available within $timeout 
	#	seconds; otherwise, returns shared arrayref, first 
	#	element is unique ID
	#
	my $foo = $q->dequeue_until($timeout);
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

=head1 DESCRIPTION

A mapped queue, similar to L<Thread::Queue>, except that as elements
are queued, they are assigned unique identifiers, which are used
to identify responses returned from the dequeueing thread. This
class provides a simple RPC-like mechanism between multiple client
and server threads, so that a single server thread can safely
multiplex requests from multiple client threads.

C<Thread::Queue::Duplex> objects encapsulate 

=over 4

=item a shared array, used as the queue (same as L<Thread::Queue>)

=item a shared scalar, used to provide unique identifier sequence
numbers

=item a shared hash, I<aka> the mapping hash, used to return responses 
to enqueued elements, using the generated uniqiue identifier as the hash key

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

=item new

The C<new> function creates a new empty queue,
and associated mapping hash.

=item enqueue LIST

Creates a shared array, pushes a unique
identifier onto the shared array, then pushes the LIST onto the array,
then pushes the shared arrayref onto the queue.

=item enqueue_urgent LIST

Same as L<enqueue>, but adds the element to head of queue, rather
than tail.

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

=item pending

Returns the number of items still in the queue.

=item respond($id [, LIST ])

Creates a new element in the mapping hash, keyed by C<$id>,
with a value set to a shared arrayref containing LIST.

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

=item wait_any(@ids)

Wait indefinitely for a response to any of the 
previously C<enqueue>'d elements specified in the
the supplied C<@ids>. Returns a hashref of available 
responses keyed by their identifiers

=item wait_any_until($timeout, @ids)

Wait upto $timeout seconds for a response to any of the 
previously C<enqueue>'d elements specified in the
the supplied C<@ids>. Returns a hashref of available 
responses keyed by their identifiers, or undef if none
available within $timeout seconds.

=item wait_all(@ids)

Wait indefinitely for a response to all the 
previously C<enqueue>'d elements specified in
the supplied C<@ids>. Returns a hashref of
responses keyed by their identifiers.

=item wait_all_until($timeout, @ids)

Wait upto $timeout seconds for a response to all the 
previously C<enqueue>'d elements specified in
the supplied C<@ids>. Returns a hashref of
responses keyed by their identifiers, or undef if all
responses are not available within $timeout seconds.

=back

=head1 SEE ALSO

L<threads>, L<threads::shared>, L<Thread::Queue>

=head1 AUTHOR, COPYRIGHT, & LICENSE

Dean Arnold, Presicient Corp. L<darnold@presicient.com>

Copyright(C) 2005, Presicient Corp., USA

Permission is granted to use this software under the same terms
as Perl itself. Refer to the Perl Artistic License for details.

=cut

use constant THRD_MAPPEDQ_Q => 0;
use constant THRD_MAPPEDQ_MAP => 1;
use constant THRD_MAPPEDQ_IDGEN => 2;

sub new {
    my $class = shift;
    my $idgen : shared = 0;
    my $obj = [
    	&share([]),
    	&share({}),
    	\$idgen
    ];
    	
    return bless $obj, $class;
}

sub enqueue {
    my $obj = shift;
#
#	NOTE: IDGEN is used as our global lock for everything
#	in order to avoid deadlocks
#
	my $id;
	{
		lock(${$obj->[THRD_MAPPEDQ_IDGEN]});
		$id = ${$obj->[THRD_MAPPEDQ_IDGEN]}++;
	}
	my @params : shared = ($id, @_);
	lock(@{$obj->[THRD_MAPPEDQ_Q]});
    push @{$obj->[THRD_MAPPEDQ_Q]}, \@params;
    cond_signal @{$obj->[THRD_MAPPEDQ_Q]};
    return $id;
}

sub enqueue_urgent {
    my $obj = shift;
#
#	NOTE: IDGEN is used as our global lock for everything
#	in order to avoid deadlocks
#
	my $id;
	{
		lock(${$obj->[THRD_MAPPEDQ_IDGEN]});
		$id = ${$obj->[THRD_MAPPEDQ_IDGEN]}++;
	}
	my @params : shared = ($id, @_);
	lock(@{$obj->[THRD_MAPPEDQ_Q]});
    unshift @{$obj->[THRD_MAPPEDQ_Q]}, \@params;
    cond_signal @{$obj->[THRD_MAPPEDQ_Q]};
    return $id;
}

sub dequeue  {
    my $obj = shift;
	lock(@{$obj->[THRD_MAPPEDQ_Q]});
    cond_wait @{$obj->[THRD_MAPPEDQ_Q]}
    	until @{$obj->[THRD_MAPPEDQ_Q]};
    my $result = shift @{$obj->[THRD_MAPPEDQ_Q]};
    cond_signal @{$obj->[THRD_MAPPEDQ_Q]} 
    	if scalar @{$obj->[THRD_MAPPEDQ_Q]};
    return $result;
}

sub dequeue_until {
    my ($obj, $timeout) = @_;

    return undef 
    	unless $timeout && ($timeout > 0);

	$timeout += time();
	my $result;

	lock(@{$obj->[THRD_MAPPEDQ_Q]});
	
	1
   	while ((! scalar @{$obj->[THRD_MAPPEDQ_Q]}) &&
   		cond_timedwait(@{$obj->[THRD_MAPPEDQ_Q]}, $timeout));

	$result = shift @{$obj->[THRD_MAPPEDQ_Q]}
   		if scalar @{$obj->[THRD_MAPPEDQ_Q]};
    cond_signal @{$obj->[THRD_MAPPEDQ_Q]} 
    	if $result && scalar @{$obj->[THRD_MAPPEDQ_Q]};
    return $result;
}

sub dequeue_nb {
    my $obj = shift;
	lock(@{$obj->[THRD_MAPPEDQ_Q]});
    return shift @{$obj->[THRD_MAPPEDQ_Q]};
}

sub pending {
    my $obj = shift;
	lock(@{$obj->[THRD_MAPPEDQ_Q]});
    return scalar @{$obj->[THRD_MAPPEDQ_Q]};
}

sub respond {
	my $obj = shift;
	my $id = shift;
	my @result : shared = @_;
	lock(%{$obj->[THRD_MAPPEDQ_MAP]});
	$obj->[THRD_MAPPEDQ_MAP]{$id} = \@result;
    cond_signal %{$obj->[THRD_MAPPEDQ_MAP]};
	return $obj;
}

sub wait {
	my $obj = shift;
	my $id = shift;	

	lock(%{$obj->[THRD_MAPPEDQ_MAP]});
	unless ($obj->[THRD_MAPPEDQ_MAP]{$id}) {
	    cond_wait %{$obj->[THRD_MAPPEDQ_MAP]}
	    	until $obj->[THRD_MAPPEDQ_MAP]{$id};
	}
    my $result = delete $obj->[THRD_MAPPEDQ_MAP]{$id};
    cond_signal %{$obj->[THRD_MAPPEDQ_MAP]}
    	if keys %{$obj->[THRD_MAPPEDQ_MAP]};
    return $result;
}
*dequeue_response = \&wait;

sub ready {
	my $obj = shift;
	my $id = shift;
#
#	no lock really needed here...
#
    return defined($obj->[THRD_MAPPEDQ_MAP]{$id}) ? $obj : undef;
}

sub available {
	my $obj = shift;

	my @ids = ();
	lock(%{$obj->[THRD_MAPPEDQ_MAP]});
	if (scalar @_) {
		map { push @ids, $_ if $obj->[THRD_MAPPEDQ_MAP]{$_}; } @_;
	}
	else {
		@ids = keys %{$obj->[THRD_MAPPEDQ_MAP]};
	}
    return scalar @ids ? wantarray ? @ids : $ids[0] : undef;
}

sub wait_until {
	my ($obj, $id, $timeout) = @_;

    return undef 
    	unless $timeout && ($timeout > 0);
	$timeout += time();
	my $result;

	lock(%{$obj->[THRD_MAPPEDQ_MAP]});
	1
    	while ((! $obj->[THRD_MAPPEDQ_MAP]{$id}) &&
    		cond_timedwait(%{$obj->[THRD_MAPPEDQ_MAP]}, $timeout));

   	$result = delete $obj->[THRD_MAPPEDQ_MAP]{$id}
   		if $obj->[THRD_MAPPEDQ_MAP]{$id};

    cond_signal %{$obj->[THRD_MAPPEDQ_MAP]}
    	if keys %{$obj->[THRD_MAPPEDQ_MAP]};
    return $result;
}
#
#	some grouped waits
#	wait indefinitely for *any* of the
#	supplied ids
#
sub wait_any {
	my $obj = shift;

	my %responses = ();
	lock(%{$obj->[THRD_MAPPEDQ_MAP]});
#
#	cond_wait isn't behaving as expected, so we need to 
#	test first, then wait if needed
#
   	map { 
   		$responses{$_} = delete $obj->[THRD_MAPPEDQ_MAP]{$_}
   			if $obj->[THRD_MAPPEDQ_MAP]{$_}; 
   	} @_;

	until (keys %responses) {
		cond_wait %{$obj->[THRD_MAPPEDQ_MAP]};
	   	map { 
	   		$responses{$_} = delete $obj->[THRD_MAPPEDQ_MAP]{$_}
	   			if $obj->[THRD_MAPPEDQ_MAP]{$_}; 
	   	} @_;
#
#	go ahead and signal...if no one's waiting, no harm
#
	    cond_signal %{$obj->[THRD_MAPPEDQ_MAP]};
	}
    return \%responses;
}
#
#	wait up to timeout for any
#
sub wait_any_until {
	my $obj = shift;
	my $timeout = shift;
	
	return undef unless $timeout && ($timeout > 0);
	$timeout += time();
	
	my %responses = ();
	lock(%{$obj->[THRD_MAPPEDQ_MAP]});
#
#	cond_wait isn't behaving as expected, so we need to 
#	test first, then wait if needed
#
   	map { 
   		$responses{$_} = delete $obj->[THRD_MAPPEDQ_MAP]{$_}
   			if $obj->[THRD_MAPPEDQ_MAP]{$_}; 
   	} @_;

	while ((! keys %responses) && 
		cond_timedwait(%{$obj->[THRD_MAPPEDQ_MAP]}, $timeout)) {

   		map { 
	   		$responses{$_} = delete $obj->[THRD_MAPPEDQ_MAP]{$_}
   				if $obj->[THRD_MAPPEDQ_MAP]{$_}; 
   		} @_;
#
#	go ahead and signal...if no one's waiting, no harm
#
	    cond_signal %{$obj->[THRD_MAPPEDQ_MAP]};
	}
    return keys %responses ? \%responses : undef;
}

sub wait_all {
	my $obj = shift;

	my %responses = ();
	lock(%{$obj->[THRD_MAPPEDQ_MAP]});
   	map { 
   		$responses{$_} = delete $obj->[THRD_MAPPEDQ_MAP]{$_}
   			if $obj->[THRD_MAPPEDQ_MAP]{$_}; 
   	} @_;
	until (scalar keys %responses == scalar @_) {
		cond_wait %{$obj->[THRD_MAPPEDQ_MAP]};

	   	map { 
	   		$responses{$_} = delete $obj->[THRD_MAPPEDQ_MAP]{$_}
	   			if $obj->[THRD_MAPPEDQ_MAP]{$_}; 
	   	} @_;
#
#	go ahead and signal...if no one's waiting, no harm
#
	    cond_signal %{$obj->[THRD_MAPPEDQ_MAP]};
	}
    return \%responses;
}

sub wait_all_until {
	my $obj = shift;
	my $timeout = shift;
	
	return undef unless $timeout && ($timeout > 0);
	$timeout += time();
	
	my %responses = ();
	lock(%{$obj->[THRD_MAPPEDQ_MAP]});
   	map { 
   		$responses{$_} = delete $obj->[THRD_MAPPEDQ_MAP]{$_}
   			if $obj->[THRD_MAPPEDQ_MAP]{$_}; 
   	} @_;
	while ((scalar keys %responses != scalar @_) && 
		cond_timedwait(%{$obj->[THRD_MAPPEDQ_MAP]}, $timeout)) {

   		map {
	   		$responses{$_} = delete $obj->[THRD_MAPPEDQ_MAP]{$_}
   				if $obj->[THRD_MAPPEDQ_MAP]{$_}; 
   		} @_;
#
#	go ahead and signal...if no one's waiting, no harm
#
	    cond_signal %{$obj->[THRD_MAPPEDQ_MAP]};
	}
    return (scalar keys %responses == scalar @_) ? \%responses : undef;
}

1;
