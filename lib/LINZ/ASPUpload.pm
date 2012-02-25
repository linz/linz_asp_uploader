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
use Log::Log4perl qw(:easy);

package ASPUploadTableDef;

use fields qw{ table id inc_key_column key_column row_tol_error row_tol_warning file columns};

sub new
{
    my ($class,$table,$id) = @_;
    my $self = fields::new($class);
    $self->{table} = $table;
    $self->{file} = undef;
    $self->{columns} = [];
    $self->{inc_key_column} = undef;
    $self->{key_column} = undef;
    $self->{row_tol_error} = undef;
    $self->{row_tol_warning} = undef;
    $self->{id} = $id;
    return $self;
}

sub add_columns
{
    my($self,@columns) = @_;
    @columns = @{$columns[0]} if ref $columns[0];
    push(@{$self->{columns}},@columns);
}

sub set_inc_key_column { $_[0]->{inc_key_column} = $_[1]; }
sub set_key_column { $_[0]->{key_column} = $_[1]; }
sub set_row_tol_error { $_[0]->{row_tol_error} = $_[1]; }
sub set_row_tol_warning { $_[0]->{row_tol_warning} = $_[1]; }
sub set_file { $_[0]->{file} = $_[1]; }

sub name { return $_[0]->{table}; }
sub id { return $_[0]->{id}; }
sub file { return $_[0]->{file}; }
sub columns { return wantarray ? @{$_[0]->{columns}} :$_[0]->{columns}; }
sub inc_key_column { return $_[0]->{inc_key_column}; }
sub key_column { return $_[0]->{key_column}; }
sub row_tol_error { return $_[0]->{row_tol_error}; }
sub row_tol_warning { return $_[0]->{row_tol_warning}; }


# #####################################################################

package ASPUploadDatasetDef;

use fields qw{ config_file tables };

sub new
{
    my($class,$config_file) = @_;
    my $self = fields::new($class);
    $self->{tables} = [];
    $self->_read_config($config_file) if $config_file;
    return $self;
}

sub _report_config_error
{
    my($self,@message) = @_;
    LOGDIE("Error reading ASP upload dataset configuration from ",
        $self->{config_file}, "\n", @message)
}

sub _config_error
{
    my($self,$errors,$fh,$message) = @_;
    push(@$errors,"Line ".$fh->input_line_number.": $message\n");
}

sub _read_config
{
    my ($self,$config_file) = @_;
    $self->{config_file} = $config_file;
    $self->{tables}=[];

    open(my $in, "<$config_file") || $self->_report_config_error("Cannot open file:$!"); 
    my $table;
    my $errors = [];
    my %tables = ();
    my $id = 0;
    while(<$in>)
    {
        next if /^\s*(\#|$)/;
        my ($command,@values) = split;
        $command = lc($command);
        if( $command eq 'table' )
        {
            my $name = shift(@values);
            $name = lc($name);
            $id++;
            $table = new ASPUploadTableDef($name,$id);
            while(my $v = shift(@values))
            {
                $v = lc($v);
                last if $v =~ /^file?$/;
                if($v =~ /^(pkey)\=(\S+)$/ )
                {
                    my $value = $2;
                    $table->set_key_column($value);
                }
                if($v =~ /^(inc_key)\=(\S+)$/ )
                {
                    my $value = $2;
                    $table->set_inc_key_column($value);
                }
                if($v =~ /^(row_tol)\=(\S+)\,(\S+)$/ )
                {
                    my $real_re = qr/^\s*(?:\d+(?:\.\d*)?|\.\d+)\s*$/;
                    my $error_tol = $2;
                    my $warn_tol = $3;
                    $self->_config_error($errors,$in,"Error tolerance is not valid for table $name")
                        if $error_tol !~ $real_re || $error_tol > 1;
                    $self->_config_error($errors,$in,"Warning tolerance is not valid for table $name")
                        if $warn_tol !~ $real_re || $warn_tol > 1;
                    $table->set_row_tol_error($error_tol);
                    $table->set_row_tol_warning($warn_tol);
                }
            }
            $self->_config_error($errors,$in,"No files defined for table $name")
                if ! @values;
            $table->set_file(@values);
            
            push(@{$self->{tables}},$table);
        }
        elsif( $table && $command eq 'column' )
        {
            $table->add_columns(join(' ',@values));
        }
        else
        {
            $self->_config_error($errors,$in,"Invalid or out of sequence command $command");
        }
    }
    close($in);
    if( @$errors )
    {
        my $nerror = @$errors;
        $self->_report_config_error("$nerror errors reading file\n",@$errors);
    }
}

sub tables { return wantarray ? @{$_[0]->{tables}} : $_[0]->{tables} }

sub is_available_in_dataset
{
    my($self,$dataset) = @_;
    my @missing = ();
    foreach my $t ( @{$self->{tables}} )
    {
        my ($status, $temp) = $t->is_available_in_dataset($dataset);
        push @missing, @$temp if @$temp;
    }
    return (@missing ? 0 : 1, \@missing);
}

sub _subset_clone 
{
    my($self,@tables) = @_;
    my $clone = fields::new(ref($self));
    $clone->{config_file}=$self->{config_file};
    $clone->{tables} = \@tables;
    return $clone;
}

sub subset
{
    my( $self, @tables ) = @_;
    @tables = @{$tables[0]} if ref $tables[0];
    my %tables = map { lc($_) => 1 } @tables;
    my @subset = grep { $tables{lc($_->name) } } $self->tables;
    return $self->_subset_clone(@subset);
}

sub excluding
{
    my( $self, @tables ) = @_;
    @tables = @{$tables[0]} if ref $tables[0];
    my %tables = map { lc($_) => 1 } @tables;
    my @subset = grep { ! $tables{lc($_->name) } } $self->tables;
    return $self->_subset_clone(@subset);
}

sub table 
{
    my ($self,$name) = @_;
    $name = lc($name);
    foreach my $t ($self->tables){ return $t if $t->name eq $name; }
    return undef;
}


# ###################################################################

package LINZ::ASPUpload;

use Log::Log4perl qw(:easy);

use File::Path;
use File::Spec;
use File::Basename;
use Archive::Extract;
use URI::Fetch;
use Cache::File;
use Digest::MD5;

use LINZ::ASPDatabase;

use fields qw{ cfg db repository dataset tmp_base keepfiles diffFunctionExists versionId};

sub new
{
    my($class,$cfg) = @_;

    my $self = fields::new($class);
    $self->{cfg} = $cfg;

    # Load the tables configuration and process any inclusions/exclusions
    
    my $dataset = new ASPUploadDatasetDef($cfg->asp_tables_config); 

    # Override for command line selection
    if( $cfg->select_tables('') =~ /\S/ )
    {
        my @requested = split(' ',$cfg->select_tables);
        $dataset = $dataset->subset(@requested);
        foreach my $t (@requested )
        {
            WARN("No definition is available for requested table $t")
                if ! $dataset->table($t);
        }
    }
    else
    {
        if( $cfg->include_tables('') =~ /\S/)
        {
            $dataset = $dataset->subset(split(' ',$cfg->include_tables));
        }
        if( $cfg->exclude_tables('') =~ /\S/)
        {
            $dataset = $dataset->excluding(split(' ',$cfg->_exclude_tables));
        }
    }

    $self->{dataset} = $dataset;
    
    # Set up the repository and the database

    $self->{db} = new LINZ::ASPDatabase($cfg);
    $self->{db}->setApplication($cfg->application_name);

   
    # Check for the base scratch directory - create it if it doesn't exist
    
    my $scratch = $cfg->tmp_base_dir;
    if( ! -d $scratch )
    {
        mkpath($scratch);
        $self->die_error("Cannot create temporary working folder $scratch") if ! -d $scratch;
    }
    $self->{tmp_base} = File::Spec->rel2abs($scratch);

    # Id for working files to ensure unique filenames.

    $self->{keepfiles} = $cfg->keep_files('') ? 1 : 0;

    return $self;
}

sub DESTROY
{
    my ($self) = @_;
}


sub cfg { return $_[0]->{cfg}};
sub db { return $_[0]->{db}; }
sub dataset { return $_[0]->{dataset}; }

sub ApplyUpdates
{
    my($self) = @_;
    
    my $filename = basename($self->cfg->asp_data_url);
    my $filepath = File::Spec->catdir($self->cfg->tmp_base_dir, $filename);
    my $cache_dir = File::Spec->catdir($self->cfg->tmp_base_dir, 'cache');
    my $cache = Cache::File->new( cache_root => $cache_dir );
    my $res = URI::Fetch->fetch($self->cfg->asp_data_url,
            Cache => $cache
    )
        || die URI::Fetch->errstr;
    
    if ($res->status == URI::Fetch::URI_OK()) {
        DEBUG("File successfully downloaded");
    }
    elsif ($res->status == URI::Fetch::URI_NOT_MODIFIED())
    {
        if ($self->cfg->force)
        {
            INFO("ASP data has not changed since last upload, but will be loaded anyway");
        }
        else
        {
            INFO("ASP data has not changed since last upload");
            return 0;
        }
    }
    else
    {
        ERROR("Unexpected result when trying to fetch ASP data. HTTP STATUS: ". $res->http_status);
        return 0;
    }
    
    open( DAT, ">$filepath" ) || die ( "Can't save file to disk: $!" );
    binmode(DAT);
    syswrite(DAT, $res->content);
    close DAT;
    
    my $archive = Archive::Extract->new( archive => $filepath );
    $archive->extract( to => $self->cfg->tmp_base_dir )
        || die "Could not extract file";
    
    my %files;
    my $outdir = $archive->extract_path;
    foreach my $f ( @{$archive->files} )
    {
        $files{$f} = File::Spec->catdir($outdir, $f);
    }

    eval 
    {
        $self->startJob($filepath);
        foreach my $t ( $self->dataset->tables )
        {
            my $file = $t->file;
            if ( !exists $files{$file} )
            {
                LOGDIE("Can't find table dataset file $file");
            }
            $self->updateTable($t, $files{$file});
        }
        $self->finishJob;
    };
    if ($@)
    {
        ERROR("$@");
        $self->db->abortJob;
    }
}

sub maintainTables
{
    my $self = shift;
    INFO("Maitaining tables");
    my $schema = $self->cfg->asp_schema;
    foreach my $t ( $self->dataset->tables )
    {
        my $table_name = $t->name;
        $self->db->do("VACUUM ANALYSE $schema.$table_name");
    }
}

sub startJob
{
    my $self = shift;
    my $file_path = shift;
    my $table_name = 'upload_detail';
    my $schema = $self->cfg->asp_schema;
    $self->db->startJob;
    
    my $sql = q(
        SELECT true
        FROM   pg_tables
        WHERE  schemaname = ?
        AND    tablename = ?
    );
    my @row = $self->db->selectArray($sql, {}, $schema, $table_name);
    if (!defined $row[0])
    {
        $sql = qq(
            CREATE TABLE $schema.$table_name (
                id SERIAL NOT NULL PRIMARY KEY,
                archive_filename TEXT NOT NULL,
                archive_md5 TEXT NOT NULL
            )
        );
        $self->db->do($sql);
    }
    
    my $file_name = basename($file_path);
    open(FILE, $file_path) || die "Can't open $file_path $!";
    binmode(FILE);
    my $sum = Digest::MD5->new->addfile(*FILE)->hexdigest();
    close FILE;
    
    $sql = qq(
        SELECT true
        FROM $schema.$table_name
        WHERE archive_filename = ?
        AND archive_md5 = ?
    );
    my @row = $self->db->selectArray($sql, {}, $file_name, $sum);
    if ($row[0])
    {
        die "ASP dataset file $file_name has laready been applied. md5_sum: $sum";
    }
    $sql = qq(
        INSERT INTO $schema.$table_name(archive_filename, archive_md5)
        VALUES (?, ?) RETURNING id
    );
    $self->db->selectArray($sql, {}, $file_name, $sum);
    $self->startVersion;
}

sub finishJob
{
    my $self = shift;
    $self->endVersion;
    $self->db->finishJob;
    $self->maintainTables if $self->cfg->maintain_tables;
}


sub startVersion
{
    my $self = shift;
    if($self->{versionId})
    {
        die "Version " . $self->{versionId} . " Has already been started";
    }
    my @row = $self->db->selectArray(
        "SELECT true FROM pg_namespace WHERE nspname = 'table_version'"
    );
    if ($row[0])
    {
        my $schema = $self->cfg->asp_schema;
        my @tables = map {$_->name} $self->dataset->tables;
        my $sql = q(
            SELECT count(*) > 0
            FROM table_version.ver_get_versioned_tables() VTB
            WHERE VTB.schema_name = 
        );
        $sql .= "'$schema' AND VTB.table_name IN ('" . join("','", @tables) . "')";
        @row = $self->db->selectArray($sql);
        if ($row[0])
        {
            my @version = $self->db->selectArray(
                "SELECT table_version.ver_create_revision('ASP upload')"
            );
            if (!$version[0])
            {
                die "Could not get version Id";
            }
            $self->{versionId} = $version[0];
        }
    }
}

sub endVersion
{
    my $self = shift;
    if ($self->{versionId})
    {
        $self->db->do("SELECT table_version.ver_complete_revision()");
    }
    $self->{versionId} = undef;
}

sub diffFunctionExists
{
    my $self = shift;
    if ($self->{diffFunctionExists})
    {
        return $self->{diffFunctionExists};
    }
    my $sql = q(
        SELECT true
        FROM   pg_proc p
        JOIN   pg_type t ON t.oid = p.prorettype
        JOIN   pg_namespace n ON n.oid =  p.pronamespace
        WHERE  n.nspname = 'bde_control'
        AND    p.proname ILIKE 'bde_applytabledifferences'
    );
    my @row = $self->db->selectArray($sql);
    if ($row[0])
    {
        $self->{diffFunctionExists} = 1;
    }
    return $self->{diffFunctionExists};
}

sub updateTable
{
    my ($self, $table, $file) = @_;
    my $table_name = $table->name;
    my $schema = $self->cfg->asp_schema;
    INFO("Starting update $schema.$table_name");
        
    my $sql = q(
        SELECT true
        FROM   pg_tables
        WHERE  schemaname = ?
        AND    tablename = ?
    );
    my @row = $self->db->selectArray($sql, {}, $schema, $table_name);
    if (!$row[0])
    {
        die "Table $table_name does not exist in schema $schema";
    }
    
    my $load_table;
    my $db_table_cols = {};
    
    open(FILE, "<$file") || die "can't open file $file: $!";
    my $header = readline FILE;
    $header =~ s/\r?\n$//;
    my @header_cols = split  /\|/, $header;
    close FILE;
    
    if ($self->diffFunctionExists)
    {
        $load_table = "tmp_${table_name}";
        $sql = qq(
            SELECT
                ATT.attname as column,
                format_type(ATT.atttypid, ATT.atttypmod) as datatype,
                ATT.attnotnull
            FROM
                pg_attribute ATT
            WHERE
                ATT.attnum > 0 AND
                NOT ATT.attisdropped AND
                ATT.attrelid = '$schema.$table_name'::REGCLASS
        );
        $db_table_cols = $self->db->selectAllHash($sql, 'column');
        
        $sql = '';
        foreach my $col (@header_cols)
        {
            my $col_def = $db_table_cols->{$col};
            LOGDIE("$col from $file does not exist in database table $table_name")
                unless $col_def;
            $sql .= ", " if $sql ne '';
            $sql .= $col_def->{column} .' '. $col_def->{datatype};
            $sql .= " NOT NULL" if $col_def->{attnotnull};
        }
        $sql = "CREATE TABLE $load_table ( $sql );";
        $self->db->do($sql);
    }
    else
    {
        $self->db->do("TRUNCATE TABLE $table_name");
        $load_table = "$schema.$table_name";
    }
    
    $sql = "COPY $load_table (". join(', ', @header_cols) . ") FROM '" .
        $file . "' DELIMITERS '|' CSV HEADER NULL AS ''";
    $self->db->do($sql);
    $self->db->do("ANALYSE $load_table");
    
    if ($self->diffFunctionExists)
    {
        my $compare_key = $table->inc_key_column;
        my $key_column = $table->key_column;
        if (defined $key_column && $key_column ne $compare_key )
        {
            $sql = '';
            foreach my $col (@header_cols)
            {
                $sql .= ",\n" if $sql ne '';
                $sql .= "TMP.${col}";
            }
            my $inc_table = "tmp_inc_${table_name}";
            $sql = qq(
                CREATE TEMP TABLE $inc_table AS
                SELECT
                    COALESCE(
                        ORG.$compare_key,
                        nextval('$schema.${table_name}_${compare_key}_seq')
                    ) AS $compare_key,
                    $sql
                FROM
                    $load_table AS TMP
                    LEFT JOIN ${schema}.${table_name} AS ORG
                    ON ORG.${key_column} = TMP.${key_column}
            );
            $self->db->do($sql);
            $self->db->do("ALTER TABLE $inc_table ALTER COLUMN $compare_key SET NOT NULL;");
            $self->db->do("ALTER TABLE $inc_table ADD PRIMARY KEY ($key_column)");
            $self->db->do("ANALYSE $inc_table");
            $self->db->do("DROP TABLE $load_table");
            $load_table = $inc_table;
            my $db_table_cols = $self->db->selectAllHash("SELECT * FROM $load_table", 'id');
            $db_table_cols = undef;
        }
        
        $self->db->do("ALTER TABLE $load_table ADD UNIQUE ($compare_key)");
        my $sql = 'SELECT bde_control.bde_applytabledifferences(' .
            "NULL, '${schema}.${table_name}'::REGCLASS, '${load_table}'::REGCLASS," .
            "'$compare_key')";
        $self->db->do($sql);
        $self->db->do("DROP TABLE $load_table");
    }
    
    if (!$self->{keepfiles})
    {
        unlink $file || die "Can't delete file $file $!";
    }
    INFO ("$schema.$table_name has been updated");
}

1;

__END__

=head1 NAME

LINZ::ASPUpload - A module to manage the a ASP upload job.

=head1 Synopsis

Module to manage the a ASP upload job.  Manages the configuration of tables
to upload, the target database, and the repository from which files are 
uploaded.

=head2 Public Functions

=over

=item $upload = new LINZ::ASPUpload($cfg);

=item $upload->ApplyUpdates

Apply ASP table updates to teh database from the given $cfg parameters

=back

=head2 Configuration items

=over

=item asp_tables_config

=item include_tables

=item exclude_tables

=item application_name

=item tmp_base_dir

=item keep_files

=item asp_schema

=item maintain_tables

=item db_connection

=item db_user

=item db_pwd 

=item db_connect_sql

=item db_upload_complete_sql

=item override_locks

=item force

=back

=back

=head2 Internal functions

=over

=item $upload->cfg

=item $upload->db

=item $upload->dataset

=item $upload->maintainTables

=item $upload->startJob

=item $upload->finishJob

=item $upload->startVersion

=item $upload->endVersion

=item $upload->diffFunctionExists

=item $upload->updateTable

=back

=head1 Classes used by LINZ::ASPUpload

=head2 ASPUploadTableDef

Holds a definition of a file for uploading.

=over

=item $def = new ASPUploadTableDef($tablename);

Creates a new table definition

=item $def->name

The name of the table

=item $def->id

The id of the table def

=item $def->file

The filename for the tabledef

=item $def->set_file($file)

Set file to be uploaded.

=item $def->columns

The columns defined for the tabledef

=item $def->add_columns($column,...)

Adds one or more column definitions to the definition.  If columns are
specified they override those in the ASP header (use with care!)

=item $def->set_inc_key_column

Set column used for the doing incremental change comparisons

=item $def->inc_key_column

The column used for the doing incremental change comparisons

=item $def->key_column

The column used for the table primary key

=item $def->set_key_column

Set column used for the table primary key

=back

=head2 ASPUploadDatasetDef

Loads and manages a set of ASPUploadDatasetDef definitions

=over

=item $datasetdef = new ASPUploadDatasetDef($config_file)

Loads configuration file containing definitions of data sets.  Will die 
if the file cannot be loaded or contains errors.

=item $datasetdef->tables

Returns a list of the ASPUploadDatasetDef items in the definition 

=item $subset = $datasetdef->subset($table1, $table2, ... )

Returns a new dataset definition containing only the specified tables
(specified by name).

=item $subset = $datasetdef->excluding($table1, $table2, ... )

Returns a new dataset definition excluding the specified tables
(specified by name).

=item $tabldef = $dataset->table($table)

Return a tabledef for the given $table parameter

=back

