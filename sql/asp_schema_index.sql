-------------------------------------------------------------------------------
--
-- $Id$
--
-- linz_asp_uploader -  LINZ ASP loader for PostgreSQL
--
-- Copyright 2011 Crown copyright (c)
-- Land Information New Zealand and the New Zealand Government.
-- All rights reserved
--
-- This program is released under the terms of the new BSD license. See the 
-- LICENSE file for more information.
--
-------------------------------------------------------------------------------
-- ASP indexes
-------------------------------------------------------------------------------

DO $SCHEMA$
BEGIN

IF EXISTS (SELECT * FROM pg_namespace where LOWER(nspname) = 'asp') THEN
    RETURN;
END IF;

SET client_min_messages TO WARNING;
SET search_path = asp, public;

-------------------------------------------------------------------------------
-- street
-------------------------------------------------------------------------------
CREATE INDEX idx_asp_street_sufi on street (sufi);

-------------------------------------------------------------------------------
-- street_part
-------------------------------------------------------------------------------
CREATE INDEX idx_asp_street_part_sufi on street_part (sufi);
CREATE INDEX idx_asp_street_part_street_sufi on street_part (street_sufi);

END;
$SCHEMA$;

