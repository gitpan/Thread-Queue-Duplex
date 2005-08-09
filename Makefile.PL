# -*- perl -*-

require 5.008;
use strict;
require ExtUtils::MakeMaker;

eval { require 5.008; } or die << 'EOD';
#######################################
#	This module requires a minimum Perl version of 5.8.0
#	Please upgrade!
#######################################
EOD

print "\nConfiguring Thread::Queue::Duplex ...\n
";

my %prereq;
my %opts =
    ('NAME' => 'Thread::Queue::Duplex',
     'VERSION_FROM' => 'lib/Thread/Queue/Duplex.pm',
     'dist'         => { 
	 	'DIST_DEFAULT' => 'all tardist',
     	'SUFFIX'       => 'gz',
	 	'COMPRESS'     => 'gzip -9vf' }
     );

if ($ExtUtils::MakeMaker::VERSION >= 5.43) {
    $opts{'AUTHOR'} = 'Dean Arnold, Presicient Corp. (darnold@presicient.com)';
    $opts{'ABSTRACT'} = 'Thread safe queue with identifiable response elements';
    $opts{'PREREQ_PM'} = \%prereq;
}

ExtUtils::MakeMaker::WriteMakefile(%opts);

package MY;

sub postamble {
    "\npm_to_blib: README\n" .
    "\nREADME: lib/Thread/Queue/Duplex.pm" .
    "\n\tperldoc -t lib/Thread/Queue/Duplex.pm >README\n\n"
}

sub libscan {
    my($self, $path) = @_;
    ($path =~ /\~$/) ? undef : $path;
}
