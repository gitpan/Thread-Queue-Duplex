use strict;
use warnings;
use vars qw($testno $loaded $srvtype);
BEGIN { 
	my $tests = 44; 
	$^W= 1; 
	$| = 1; 
	print "Note: some tests have significant delays...\n";
	print "1..$tests\n"; 
}

END {print "not ok $testno\n" unless $loaded;}

use threads;
use threads::shared;
use Thread::Queue::Duplex;

$srvtype = 'init';

sub report_ok {

	print "ok $testno ", shift, " for $srvtype\n";
	$testno++;
}

sub report_fail {

	print "not ok $testno ", shift, " for $srvtype\n";
	$testno++;
}

sub run_dq {
	my $q = shift;
#
#	test each dequeue method
#
	while (1) {
		my $left = $q->pending;

		my $req = $q->dequeue;

		last if ($req->[1] eq 'stop');
		sleep($req->[2])
			if ($req->[1] eq 'wait');

		my $id = shift @$req;
		$q->respond($id, @$req);
	}
}

sub run_nb {
	my $q = shift;
#
#	test each dequeue method
#
	while (1) {
		my $req = $q->dequeue_nb;
		sleep 1, next
			unless $req;

		last if ($req->[1] eq 'stop');

		sleep($req->[2])
			if ($req->[1] eq 'wait');
		my $id = shift @$req;
		$q->respond($id, @$req);
	}
}

sub run_until {
	my $q = shift;
#
#	test each dequeue method
#
	my $timeout = 2;
	while (1) {

		my $req = $q->dequeue_until($timeout);
		sleep 1, next
			unless $req;

		last if ($req->[1] eq 'stop');

		sleep($req->[2])
			if ($req->[1] eq 'wait');
		my $id = shift @$req;
		$q->respond($id, @$req);
	}
}

$testno = 1;

report_ok('load module');
#
#	create queue
#	spawn server thread
#	execute various requests
#	verify responses
#
#	test constructor
#
my $q = new Thread::Queue::Duplex;

report_ok('create queue');
#
#	test different kinds of dequeue
#
my @servers = (\&run_dq, \&run_nb, \&run_until);
my @types = ('normal', 'nonblock', 'timed');

my ($result, $id, $server);

my $start = $ARGV[0] || 0;
foreach ($start..$#servers) {
	$server = threads->new($servers[$_], $q);
	$srvtype = $types[$_];
#
#	test enqueue
#
	$id = $q->enqueue('foo', 'bar');
	defined($id) ? 
		report_ok('enqueue()') :
		report_fail('enqueue()');
#
#	test ready(); don't care about outcome
#	(prolly need eval here)
#
	$result = $q->ready($id);
	report_ok('ready()');
#
#	test wait()
#
	$result = $q->wait($id);
	
	(defined($result) && 
		($result->[0] eq 'foo') &&
		($result->[1] eq 'bar')) ? 
		report_ok('wait()') :
		report_fail('wait()');
#
#	test dequeue_response
#
	$id = $q->enqueue('foo', 'bar');
	$result = $q->dequeue_response($id);
	(defined($result) && 
		($result->[0] eq 'foo') &&
		($result->[1] eq 'bar')) ? 
		report_ok('dequeue_response()') :
		report_fail('dequeue_response()');
#
#	test wait_until
#
	$id = $q->enqueue('wait', 3);
	$result = $q->wait_until($id, 1);
	defined($result) ?
		report_fail('wait_until() expires') :
		report_ok('wait_until() expires');

	$result = $q->wait_until($id, 5);
	defined($result) ?
		report_ok('wait_until()') :
		report_fail('wait_until()');
#
#	test wait_any: need to queue up several
#
	my %ids = ();

	map { $ids{$q->enqueue('foo', 'bar')} = 1; } (1..10);
#
#	repeat here until all ids respond
#
	my $failed;
	while (keys %ids) {
		$result = $q->wait_any(keys %ids);
		$failed = 1,
		last
			unless defined($result) &&
				(ref $result) &&
				(ref $result eq 'HASH');
		map { 
			$failed = 1 
				unless delete $ids{$_}; 
		} keys %$result;
		last
			if $failed;
	}
	$failed ?
		report_fail('wait_any()') :
		report_ok('wait_any()');
#
#	test wait_any_until
#
	%ids = ();

	map { $ids{$q->enqueue('wait', '3')} = 1; } (1..10);
	$failed = undef;

	$result = $q->wait_any_until(1, keys %ids);
	if ($result) {
		report_fail('wait_any_until()');
	}
	else {
		while (keys %ids) {
			$result = $q->wait_any_until(5, keys %ids);
			$failed = 1,
			last
				unless defined($result) &&
					(ref $result) &&
					(ref $result eq 'HASH');
			map {
				$failed = 1 
					unless delete $ids{$_}; 
			} keys %$result;
			last
				if $failed;
		}
		$failed ?
			report_fail('wait_any_until()') :
			report_ok('wait_any_until()');
	}
#
#	test wait_all
#
	%ids = ();
	map { $ids{$q->enqueue('foo', 'bar')} = 1; } (1..10);
#
#	test available()
#
	sleep 1;
	my @avail = $q->available;
	scalar @avail ? 
		report_ok('available (array)') :
		report_fail('available (array)');

	my $id = $q->available;
	$id ? 
		report_ok('available (scalar)') :
		report_fail('available (scalar)');

	$id = keys %ids;
	@avail = $q->available($id);
	scalar @avail ? 
		report_ok('available (id)') :
		report_fail('available (id)');
#
#	make sure all ids respond
#
	$result = $q->wait_all(keys %ids);
	unless (defined($result) &&
		(ref $result) &&
		(ref $result eq 'HASH') &&
		(scalar keys %ids == scalar keys %$result)) {
		report_fail('wait_all()');
	}
	else {
		map { $failed = 1 unless delete $ids{$_}; } keys %$result;
		($failed || scalar %ids) ?
			report_fail('wait_all()') :
			report_ok('wait_all()');
	}
#
#	test wait_all_until
#
	%ids = ();
	map { $ids{$q->enqueue('wait', '3')} = 1; } (1..10);
#
#	make sure all ids respond
#
	$result = $q->wait_all_until(1, keys %ids);
	if (defined($result)) {
		report_fail('wait_all_until()');
	}
	else {
	# may need a warning print here...
		$result = $q->wait_all_until(50, keys %ids);
		map { $failed = 1 unless delete $ids{$_}; } keys %$result;
		($failed || scalar %ids) ?
			report_fail('wait_all_until()') :
			report_ok('wait_all_until()');
	}
#
#	kill the thread; also tests urgent i/f
#
	$q->enqueue_urgent('stop') ?
		report_ok('enqueue_urgent()') :
		report_fail('enqueue_urgent()');
	$server->join;
}	#end foreach server method

$testno--;
$loaded = 1;

