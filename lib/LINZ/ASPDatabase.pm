################################################################################
#
# $Id$
#
# linz_asp_uploader -  LINZ ASP loader for PostgreSQL
#
# Copyright 2012 Crown copyright (c)
# Land Information New Zealand and the New Zealand Government.
# All rights reserved
#
# This program is released under the terms of the new BSD license. See the 
# LICENSE file for more information.
#
################################################################################

use strict;
use DBI;

=head1 LINZ::ASPDatabase

Interface between the ASPUpload process and a database

=head1 Version

Version: $Id$

=over

=item $db = new LINZ::ASPDatabase($cfg)

Creates a new ASPDatabase object for uploading ASP table data

The following configuration functions are used:

=over

=item  $cfg->db_connection

The connection string (minus dbi:Pg:)

=item  $cfg->db_user

The database user

=item  $cfg->db_pwd 

The database password

=item  $cfg->db_connect_sql

A set of ";" separated SQL commands that are run once the connection is
established. 

=item  $cfg->db_upload_complete_sql

A set of ";" separated SQL commands taht are after a successful upload 
(one that has been applied to at least one table). Each sql command 
can be preceded by a conditional statement of the form

   "if" [any|all] [level0|level0_dataset] table ... table [loaded|affected] "?"


=item   $cfg->override_locks

Override any existing locks on files when doing the update. This will also
override constraints on allowing concurrent uploads.

=item   $cfg->asp_schema

The schema for the database session.

=back

=item $success = $db->setApplication($app_name)

Sets the application name for the SQL session. Returns true if this was
successful

=item $db->startJob()

Tries to gain a lock on the ASP database and if successful starts the database
job, by starting a transaction and executing any configured start SQL statements

=item $db->endJob()

End the database job by committing the transaction, executing any configured
end SQL statements and releasing the job lock

=item $bool = $db->jobCreated()

Returns true if a job has been successfully created

=item $db->maintain

Will run garbage collection and analyse on the ASP database.

=cut

package LINZ::ASPDatabase;

use Log::Log4perl qw(:easy :levels get_logger);
use fields qw{_jobCreated _connection _user _pwd _dbh _pg_server_version _error _startSql _finishSql _overrideLocks _intransaction _locktimeout _allowConcurrent schema};


my $asp_magic_number = 484912935;

my %pg_log_message_map = (
    DEBUG   => 'debug',
    DEBUG1  => 'debug',
    DEBUG2  => 'debug',
    DEBUG3  => 'debug',
    DEBUG4  => 'debug',
    DEBUG5  => 'debug',
    LOG     => 'debug',
    NOTICE  => 'debug',
    INFO    => 'info',
    WARNING => 'warn'
);


my %log_pg_message_map = (
    $OFF   => 'ERROR',
    $FATAL => 'ERROR',
    $ERROR => 'ERROR',
    $WARN  => 'WARNING',
    $INFO  => 'INFO',
    $DEBUG => 'DEBUG',
    $TRACE => 'DEBUG5',
    $ALL   => 'DEBUG5',
);

sub new
{
    my($class,$cfg) = @_;
    my $self = fields::new($class);
    $self->{_connection} = $cfg->db_connection;
    $self->{_user} = $cfg->db_user;
    $self->{_pwd} = $cfg->db_pwd;
    $self->{_startSql} = $cfg->db_connect_sql;
    $self->{_finishSql} = $cfg->db_upload_complete_sql;
    $self->{_overrideLocks} = $cfg->override_locks(0) ? 1 : 0;
    $self->{_locktimeout} = $cfg->table_exclusive_lock_timeout(60)+0;
    $self->{_allowConcurrent} = $cfg->allow_concurrent_uploads(0);
    $self->{_error} = 0;
    $self->{_jobCreated} = 0;

    $self->{schema} = $cfg->asp_schema;

    $self->{_dbh} = undef;
    $self->{_intransaction} = 0;

    my $dbh = DBI->connect("dbi:Pg:".$self->{_connection}, 
        $self->{_user}, $self->{_pwd}, 
        {
            AutoCommit    =>1,
            PrintError    =>1,
            PrintWarn     =>1,
            RaiseError    =>1,
            pg_errorlevel =>2,
        }
    )
       || die "Cannot connect to database\n",DBI->errstr;
    
    my $pg_server_version = $dbh->{'pg_server_version'};
    if ( $pg_server_version =~ /\d/ )
    {
        $self->{_pg_server_version} = $pg_server_version;
    }
    else
    {
        WARN "WARNING: no pg_server_version!  Assuming >= 8.4";
        $self->{_pg_server_version} = 80400;
    }
    
    if ( $self->{_pg_server_version} >= 90000 )
    {
        my $row = $dbh->selectcol_arrayref("SELECT pg_is_in_recovery()");
        if ($$row[0])
        {
            die "PostgreSQL is still in recovery after a database crash or ".
                "you are connected to a read-only slave";
        }
    }
    
    $dbh->do("set search_path to ".$self->{schema}.", public");
 
    $self->{_dbh} = $dbh;

    my $logger = get_logger();
    my $pg_msg_level = $log_pg_message_map{$logger->level};
    $dbh->do("SET client_min_messages = $pg_msg_level") if $pg_msg_level;
    
    return $self;
}

sub DESTROY
{
    my($self) = @_;
    $self->finishJob;
    if( $self->_dbh )
    {
        $self->_commitTransaction;
        $self->_dbh->disconnect;
    }
}

sub maintain
{
    my($self) = @_;
    $self->do("VACUUM ANALYSE") ||
        ERROR "Cannot vacuum database\n", $self->_dbh->errstr,"\n";
}

sub startJob
{
    my ($self) = @_;
    if ( !$self->{_overrideLocks} )
    {
        my $row = $self->selectArray("SELECT pg_try_advisory_lock($asp_magic_number)");
        if (!$$row[0])
        {
            die "An ASP uploader job is still in progress";
        }
    }
    $self->_beginTransaction;
    $self->_runSQLBlock($self->{_startSql});
    $self->{_jobCreated} = 1;
}

sub finishJob
{
    my ($self) = @_;
    return if ! $self->{_jobCreated};
    $self->_runFinishSql;
    $self->_commitTransaction;
    my $row = $self->selectArray("SELECT pg_advisory_unlock($asp_magic_number)");
    if (!$$row[0])
    {
        die "Could not unlock ASP uploader job";
    }
    $self->{_jobCreated} = 0;
}

sub abortJob
{
    my($self) = @_;
    my $result;
    if ( $self->{_intransaction} )
    {
        $result = $self->_dbh->rollback;
        $self->{_intransaction} = 0;
    }
    $self->{_jobCreated} = 0;
    return $result;
}

sub jobCreated
{
    my($self) = @_;
    return $self->{_jobCreated};
}

sub setApplication
{
    my($self,$app_name) = @_;
    my $result = 0;
    if ( $self->{_pg_server_version} >= 90000 )
    {
        my $rv = $self->do("SET application_name='$app_name'");
        $result = 1 if (defined $rv);
    }
    return $result;
}

sub _dbh { return $_[0]->{_dbh} }

sub _runSQLBlock
{
    my ($self, $sql_block) =  @_;
    return if ! $sql_block;
    my $id;
    foreach my $cmd (grep {/\S/} split(/\;\n?/,$sql_block))
    {
        eval
        {
            $self->do($cmd);
        };
        if ($@)
        {
            die "Cannot run SQL command: $cmd\n", $self->_dbh->errstr;
        }
    }
}



sub _runFinishSql
{
    my($self) = @_;
    return if ! $self->jobCreated;
    my $sql = $self->{_finishSql};
    foreach my $cmd (grep {/\S/} split(/\;/,$sql))
    {
        if( $cmd =~ /^\s*if\s+
                        (
                            (?:any\s+|all\s+|)?
                            (?:level_0(?:_dataset)?\s+)?
                        )
                        (
                            \w+(?:\s+\w+)*?
                        )
                        (
                            \s+(?:loaded|affected)
                        )?
                        \s*\?\s*(.*?)\s*$/ixs)
        {
            my $tables = $2;
            my $test = $1.$3;
            $cmd = $4;
            $test =~ s/^\s+//;
            $test =~ s/\s+$//;
            $test =~ s/\s+/ /;
            next if ! $self->tablesAffected($test,$tables);
        }
        eval
        {
            $self->do($cmd);
        };
        if ($@)
        {
            die "Cannot run finishing SQL: $cmd: ", $self->_dbh->errstr;
        }
    }
}

sub _setDbMessageHandler
{
    my $self = shift;
    $SIG{__WARN__} = sub { &_dbMessageHandler($self, @_); };
}

sub _clearDbMessageHandler
{
    $SIG{__WARN__} = undef;
}

sub _dbMessageHandler
{
    my $self = shift;
    my $db_message = shift;
    $db_message =~ s/\r\n/ /g;
    $db_message =~ s/\n/ /g;
    my ($type, $text, $extra) = $db_message
        =~ /^(\w+)\:(?:\s+0{5}\:)?\s+(.*?)\s*((?:CONTEXT|LOCATION)\:(?:.*))?$/;
    my $logger = get_logger();
    my $msg_func = $pg_log_message_map{$type};
    if ($msg_func)
    {
        $logger->$msg_func($text);
        if ($extra)
        {
            my $level = $msg_func eq 'warn' ? $msg_func : 'debug';
            $logger->$level($extra);
        }
    }
    else
    {
        die $db_message;
    }
}

sub _beginTransaction
{
    my($self) = @_;
    $self->_commitTransaction;
    $self->_dbh->begin_work;
    $self->{_intransaction} = 1;
}

sub do
{
    my ($self, $sql) = @_;
    $self->_setDbMessageHandler;
    DEBUG("Running: $sql");
    my $rv = $self->_dbh->do($sql);
    $self->_clearDbMessageHandler;
    return $rv;
}

sub selectArray
{
    my ($self, $sql, $attr, @bind_values) = @_;
    $self->_setDbMessageHandler;
    DEBUG("Running: $sql");
    my @row_ary = $self->_dbh->selectrow_array($sql, $attr, @bind_values);
    $self->_clearDbMessageHandler;
    wantarray ? @row_ary : \@row_ary;
}

sub selectAllHash
{
    my ($self, $sql, $key) = @_;
    $self->_setDbMessageHandler;
    DEBUG("Running: $sql");
    my $row_hash = $self->_dbh->selectall_hashref($sql, $key);
    $self->_clearDbMessageHandler;
    return $row_hash;
}

sub _commitTransaction
{
    my($self) = @_;

    if( $self->{_intransaction} )
    {
        $self->_dbh->commit;
        $self->{_intransaction} = 0;
    }
}

1;
