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
use warnings;

use Config;
use inc::MyBuilder;

my $requires = {
    'Log::Log4perl'                    => 0,
    'Log::Dispatch'                    => 0,
    'Log::Dispatch::FileRotate'        => 0,
    'Log::Dispatch::Email::MailSender' => 0,
    'LINZ::Config'                     => 0,
    'Archive::Extract'                 => 0,
    'DBD::Pg'                          => 0,
    'URI::Fetch'                       => 0,
    'Cache::File'                      => 0,
    'Digest::MD5'                      => 0,
};

my $build =  inc::MyBuilder->new(
    module_name        => 'LINZ::ASP::Uploader',
    license            => 'bsd',
    dist_author        => 'LINZ',
    dist_version_from  => 'bin/linz_asp_uploader.pl',
    dist_abstract      => 'Software for loading LINZ ASP files into a PostgreSQL database',
    sign               => 1,
    requires           => $requires,
	create_makefile_pl => 'small',
);

$build->add_build_element('conf');
$build->add_build_element('sql');

$build->create_build_script;
