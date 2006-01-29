package Thread::Queue::Queueable;
#
#	abstract class to permit an object to be
#	marshalled in some way before pushing onto
#	a Thread::Queue::Duplex queue
#
require 5.008;

use threads;
use threads::shared;

use strict;
use warnings;

our $VERSION = '0.90';

=head1 NAME

Thread::Queue::Queueable - abstract class for marshalling/unmarshalling
an element as it is enqueued or dequeued from a Thread::Queue::Duplex queue

=head1 SYNOPSIS

	use Thread::Queue::Queueable;
	use base qw(Thread::Queue::Queueable);
	#
	#	implement onEnqueue method
	#	(default implementation shown)
	#
	sub onEnqueue {
		my $obj = shift;
	#
	#	capture class name, and create shared
	#	version of object
	#
		return $obj->isa('ARRAY') ?
			(ref $obj, share([ @$obj ])) :
			(ref $obj, share({ %$obj }));
	}
	#
	#	implement onDequeue method
	#	(default implementation shown)
	#
	sub onDequeue {
		my ($class, $obj) = @_;
	#
	#	reconstruct as non-shared
	#
		$obj = (ref $obj eq 'ARRAY') ? [ @$obj ] : { %$obj };
		bless $obj, $class;
		return $obj;
	}
	#
	#	permit the object to be reconstructed on dequeueing
	#
	sub onCancel {
		my $obj = shift;
		return 1;
	}
	#
	#	curse (ie, unbless) the object into a shared structure
	#
	sub curse {
		my $obj = shift;

		if ($obj->isa('HASH')) {
			my %cursed : shared = ();
			$cursed{$_} = $obj->{$_}
				foreach (keys %$obj);
			return \%cursed;
		}

		my @cursed : shared = ();
		$cursed[$_] = $obj->[$_]
			foreach (0..$#$obj);
		return \@cursed;
	}
	#
	#	redeem (ie, rebless) the object into
	#	the class
	#
	sub redeem {
		my ($class, $obj) = @_;

		if (ref $obj eq 'HASH') {
			my $redeemed = {};
			$redeemed->{$_} = $obj->{$_}
				foreach (keys %$obj);
			return bless $redeemed, $class;
		}

		my $redeemed = [];
		$redeemed->[$_] = $obj->[$_]
			foreach (0..$#$obj);
		return bless $redeemed, $class;
	}

=head1 DESCRIPTION

Thread::Queue::Queueable (I<aka TQQ>) provides abstract methods to be invoked
whenever an object is enqueued or dequeued, in either the request
or response direction, on a L<Thread::Queue::Duplex> (I<TQD>) queue.

The primary purpose is to simplify application logic so that
marshalling/unmarhsalling of objects between threads is performed
automatically. In addition, when subclassed, the application class
can modify or add logic (e.g., notifying a server thread object
to update its reference count when a wrapped object is passed between
threads - see L<DBIx::Threaded> for an example).

=head1 FUNCTIONS AND METHODS

=over 8

=item onEnqueue($obj)

Called by TQD's C<enqueue()>, C<enqueue_urgent()>, and
C<respond()> methods. The default implementation L<curse>s the input
object into either a shared array or shared hash, and returns a list
consisting of the object's class name, and the cursed object.

=item onDequeue($class, $template_obj)

Called by TQD's C<dequeue()>, C<dequeue_nb()>,
C<dequeue_until()> methods, as well as the various response side dequeueing
methods (e.g., C<wait()>, C<wait_until()>, C<wait_all()>, etc.).
The default implementation redeems the input object by calling
L<redeem>() to copy the input shared arrayref or hashref
into a nonshared equivalent, and then blessing it into the specified class,
returning the redeemed object.

=item onCancel($obj)

Called by TQD's C<cancel()> and C<cancel_all()>,
as well as the C<respond()> method when a cancelled operation is detected.
The default is a pure virtual function.

=item curse($obj)

Called by TQD'd various C<enqueue()> and C<respond()> functions
when the TQQ object is being enqueue'd. Should return an unblessed,
shared version of the input object. Default returns a shared
arrayref or hashref, depending on $obj's base structure, with
copies of all scalar members.
B<Note> that objects with more complex members will need to
implement an object specific C<curse()> to do any deepcopying,
including C<curse()>ing any subordinate objects.

=item redeem($class, $obj)

Called by TQD's various C<dequeue> and C<wait> functions to
"redeem" (i.e., rebless) the object into its original class.
Default creates non-shared copy of the input $obj structure,
copying its scalar contents, and blessing it into $class.
B<Note> that objects with complex members need to implement
an object specific C<redeem()>, possibly recursively
invoking C<redeem()> on subordinate objects I<(be careful
of circular references!)>

=back

=head1 SEE ALSO

L<Thread::Queue::Duplex>, L<threads>, L<threads::shared>, L<Thread::Queue>

=head1 AUTHOR, COPYRIGHT, & LICENSE

Dean Arnold, Presicient Corp. L<darnold@presicient.com>

Copyright(C) 2005, Presicient Corp., USA

Permission is granted to use this software under the same terms
as Perl itself. Refer to the Perl Artistic License for details.

=cut

sub onEnqueue {
	my $obj = shift;
#
#	capture class name, and create cursed
#	version of object
#
	return (ref $obj, $obj->curse());
}
#
#	permit the object to be reconstructed on dequeueing
#
sub onDequeue {
	my ($class, $obj) = @_;
#
#	reconstruct as non-shared by redeeming
#
	return $class->redeem($obj);
}
#
#	permit the object to be reconstructed on dequeueing
#
sub onCancel {
	my $obj = shift;
	return 1;
}
#
#	curse the object into a shared variable
#
sub curse {
	my $obj = shift;
#
#	if we're already shared, don't share again
#
	return $obj if threads::shared::_id($obj);

	if ($obj->isa('HASH')) {
		my %cursed : shared = ();
		$cursed{$_} = $obj->{$_}
			foreach (keys %$obj);
		return \%cursed;
	}

	my @cursed : shared = ();
	$cursed[$_] = $obj->[$_]
		foreach (0..$#$obj);
	return \@cursed;
}
#
#	redeem (ie, rebless) the object into
#	the class
#
sub redeem {
	my ($class, $obj) = @_;
#
#	if object is already shared, just rebless it
#	NOTE: we can only do this when threads::shared::_id() is defined
#
	return bless $obj, $class
		if threads::shared->can('_id') && threads::shared::_id($obj);
#
#	we *could* just return the blessed object,
#	which would be shared...but that might
#	not be the expected behavior...
#
	if (ref $obj eq 'HASH') {
		my $redeemed = {};
		$redeemed->{$_} = $obj->{$_}
			foreach (keys %$obj);
		return bless $redeemed, $class;
	}

	my $redeemed = [];
	$redeemed->[$_] = $obj->[$_]
		foreach (0..$#$obj);
	return bless $redeemed, $class;
}

1;