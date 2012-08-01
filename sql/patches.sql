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

SELECT _patches.apply_patch(
    'ASP - 1.0.1: Fix ASP table permissions',
    '
DO $RIGHTS$
DECLARE
   v_table     NAME;
BEGIN
    FOR v_table IN
        SELECT
            NSP.nspname || ''.'' || CLS.relname
        FROM
            pg_class CLS,
            pg_namespace NSP
        WHERE
            CLS.relnamespace = NSP.oid AND
            NSP.nspname IN (''asp'') AND
            CLS.relkind = ''r''
        ORDER BY
            1
    LOOP
        EXECUTE ''ALTER TABLE '' || v_table || '' OWNER TO bde_dba'';
        EXECUTE ''GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE '' || v_table || '' TO bde_admin'';
        EXECUTE ''GRANT SELECT ON TABLE '' || v_table || '' TO bde_user'';
    END LOOP;
END;
$RIGHTS$
'
);

