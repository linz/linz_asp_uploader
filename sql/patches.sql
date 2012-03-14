-------------------------------------------------------------------------------
--
-- $Id$
--
-- linz_asp_uploader -  LINZ ASP loader for PostgreSQL
--
-- Copyright 2012 Crown copyright (c)
-- Land Information New Zealand and the New Zealand Government.
-- All rights reserved
--
-- This program is released under the terms of the new BSD license. See the 
-- LICENSE file for more information.
--
--------------------------------------------------------------------------------
-- Patches to apply to ASP system. Please note that the order of patches listed
-- in this file should be done sequentially i.e Newest patches go at the bottom
-- of the file. 
--------------------------------------------------------------------------------
SET client_min_messages TO WARNING;

SELECT _patches.apply_patch(
    'ASP - 1.0.0: Apply indexes',
    '
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
'
);

