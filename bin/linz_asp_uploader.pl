#!usr/bin/perl
################################################################################
#
# $Id$
#
# linz_asp_uploader -  LINZ ASP loader for PostgreSQL
#
# Copyright 2011 Crown copyright (c)
# Land Information New Zealand and the New Zealand Government.
# All rights reserved
#
# This program is released under the terms of the new BSD license. See the 
# LICENSE file for more information.
#
################################################################################
use strict;  

our $VERSION = '1.0.0';

use FindBin;
use lib $FindBin::Bin;
use Getopt::Long;

use Log::Log4perl qw(:easy :levels get_logger);
use Log::Log4perl::Layout;

use LINZ::Config;
use LINZ::ASPUpload;

@ARGV || help(0);

# Main program controls

my $verbose = 0;          # Dry run only - print out files to be updated
my $keep_files = 0;       # Keep working files - for testing
my $force = 0;
my $cfgext = '';          # Alternative configuration
my $cfgpath = '~/config'; # Configuration path
my $showhelp = 0;         # Show help
my $listing_file = '';
my $maintain_tables = 0;
my $logger;

GetOptions (
    "help|h" => \$showhelp,
    "config-extension|x=s" => \$cfgext,
    "config-path|c=s" => \$cfgpath,
    "keep-files|k" => \$keep_files,
    "maintain-tables|m" => \$maintain_tables,
    "force|f" => \$force,
    "verbose|v" => \$verbose,
    "listing_file|l=s" => \$listing_file,
    )
    || help(0);

help(1) if $showhelp;

my $of;
if($listing_file)
{
    open($of, ">", $listing_file) ||
        die "Can't not write to listing file $listing_file: $!\n";
    select($of);
};

eval
{
    my $options = 
    {
        _configpath   => $cfgpath,
        _configextra  => $cfgext,
        verbose       => $verbose,
        keep_files    => $keep_files,
        force         => $force,
        maintain_tables => $maintain_tables,
        select_tables => join(' ',@ARGV),
    };

    my $cfg = new LINZ::Config($options);
    
    my $layout = Log::Log4perl::Layout::PatternLayout->new("%d %p> %F{1}:%L - %m%n");
    my $log_config = $cfg->log_settings;
    Log::Log4perl->init(\$log_config);
    $logger = get_logger("");
    
    if($listing_file)
    {
        my $file_appender = Log::Log4perl::Appender->new(
            "Log::Dispatch::FileRotate",
            name      => "listing_file_log",
            filename  => $listing_file,
            mode      => "append",
            min_level => 'debug',
            max       => 99,
        );
        $file_appender->layout($layout);
        $logger->add_appender( $file_appender );
        DEBUG("File logging turned on");
    }
    
    if($verbose)
    {
        my $stdout_appender = Log::Log4perl::Appender->new(
            "Log::Log4perl::Appender::Screen",
            name      => "verbose_screen_log",
        );
        $stdout_appender->layout($layout);
        $logger->add_appender($stdout_appender);
    }

    my $upload = new LINZ::ASPUpload($cfg);

    $upload->ApplyUpdates();
};
if( $@ )
{
    ERROR("$@");
}

INFO("Duration of job: ". runtime_duration());
exit;

sub runtime_duration
{
    my $duration = time() - $^T;
    my $str;
    my $day;
    my $hour;
    my $min;
    my $sec;
    {
        use integer;
        $min   = $duration / 60;
        $sec   = $duration % 60;
        $hour  = $min      / 60;
        $min   = $min      % 60;
        $day   = $hour     / 24;
        $hour  = $hour     % 24;
    }
    
    if ($day) {
        $str = sprintf("%dd:%02d:%02d:%02d",$day, $hour, $min, $sec);
    }
    else
    {
        $str = sprintf("%02d:%02d:%02d", $hour, $min, $sec);
    }
    return $str;
}

sub help
{
    my($full) = @_;
    my $level = $full ? 2 : 99;
    my $sections = 'Syntax';
    require Pod::Usage;
    Pod::Usage::pod2usage({
        -verbose=>$level,
        -sections=>$sections,
        -exitval=>'NOEXIT' 
    });
    exit;
}
__END__

=head1 linz_asp_uploader.pl

Script for uploading LINZ ASP data into PostgreSQL.

=head1 Version

Version: $Id$

=head1 Syntax

  perl linz_asp_uploader.pl [options..] [tables..]

If no options are a brief help message is displayed. If tables are included,
then only those tables will be updated.

The list of tables is optional and defines the subset of the tables that will
be updated.  Only tables defined in the configuration will be updated - 
any additional tables listed are silently ignored(!)

Options:

=over

=item -config-path or -c I<cfgpath>

=item -config-extension or -x  I<cfgext>

=item -listing_file or -l I<listing_file>

=item -keep-files or -k

=item -force or -f

=item -maintain-tables or -m

=item -verbose or -v

=item -help or -h

=back

=head1 Options

=over 

=item -config-path or -c I<cfgpath>

Select the configuration file that will be used.  Default is
~/config/linz_asp_uploader.conf, where ~ is the directory in which the
linz_asp_uploader.pl script is located.

=item -config-extension or -x  I<cfgext>

Extra configuration extension.  Overrides selected configuration items with
values from asp.cfg.I<cfgext> 

=item -listing_file or -l I<listing_file>

Specifies a file for reporting.  Most reporting is written to the database and
sent as email notifications as defined in the configuration.  
If the verbose option is specified, or if the
email server is unavailable, then this may be sent to the standard output
file.  The I<listing_file> can be used in place of standard output.

=item -keep-files or -k

Keeps the files generated during the upload rather than deleting them - 
for debugging use.

=item -maintain-tables or -m

After a job has been successfully run and the database has been updated, the
asp database tables will be garbage collected and analysed

=item -force or -f

Force the download and application of asp data even if the file has already been
applied to the database.

=item -verbose or -v

Specifies that messages will be sent to standard output (or the report file)
as well as to the database.


=back
