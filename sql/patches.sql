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

SELECT _patches.apply_patch(
    'ASP - 1.0.2: Rebuild primary keys using versioned table column key',
    '
DO $PATCH$
DECLARE
    v_schema_name             TEXT;
    v_table_name              TEXT;
    v_version_key_column      TEXT;
    v_table_primary_key       TEXT;
    v_table_primary_key_name  TEXT;
    v_table_unique_constraint TEXT;
    v_table_unqiue_index      TEXT;
BEGIN
    FOR
        v_schema_name,
        v_table_name,
        v_version_key_column,
        v_table_primary_key,
        v_table_primary_key_name,
        v_table_unique_constraint,
        v_table_unqiue_index
    IN
        WITH t AS (
            SELECT
                CLS.oid AS table_oid,
                TBL.schema_name,
                TBL.table_name,
                TBL.key_column AS version_key_column,
                string_agg(DISTINCT ATT.attname, '','') as table_primary_key,
                string_agg(DISTINCT CONP.conname, '','') AS table_primary_key_name,
                string_agg(DISTINCT CONU.conname, '','') AS table_unique_constraint
            FROM
                table_version.ver_get_versioned_tables() AS TBL,
                pg_namespace NSP,
                pg_index IDX,
                pg_attribute ATT,
                pg_class CLS
                JOIN pg_constraint CONP ON (CONP.conrelid = CLS.oid AND CONP.contype = ''p'')
                LEFT JOIN pg_constraint CONU ON (CONU.conrelid = CLS.oid AND CONU.contype = ''u'')
            WHERE
                TBL.schema_name = ''asp'' AND
                NSP.nspname  = TBL.schema_name AND
                CLS.relname  = TBL.table_name AND
                NSP.oid      = CLS.relnamespace AND
                IDX.indrelid = CLS.oid AND
                ATT.attrelid = CLS.oid AND 
                ATT.attnum   = any(IDX.indkey) AND
                IDX.indisprimary
            GROUP BY
                CLS.oid,
                TBL.schema_name,
                TBL.table_name,
                TBL.key_column
            HAVING
                TBL.key_column <> string_agg(ATT.attname, '','')
        )
        SELECT
            t.schema_name,
            t.table_name,
            t.version_key_column,     
            t.table_primary_key,
            t.table_primary_key_name,
            t.table_unique_constraint,
            CLS.relname as table_unqiue_index
        FROM
            t
            LEFT JOIN pg_index IDX ON (IDX.indrelid = t.table_oid AND IDX.indisunique AND NOT IDX.indisprimary)
            LEFT JOIN pg_class CLS ON (IDX.indexrelid = CLS.oid)
            LEFT JOIN pg_attribute ATT ON (ATT.attrelid = t.table_oid AND ATT.attname = t.version_key_column AND ATT.attnum = ANY(IDX.indkey))
        WHERE
            ATT.attname IS NOT NULL
        ORDER BY
            t.schema_name,
            t.table_name
    LOOP
        EXECUTE ''ALTER TABLE '' || v_schema_name || ''.'' || v_table_name || '' DROP CONSTRAINT  '' || v_table_primary_key_name;
        EXECUTE ''ALTER TABLE '' || v_schema_name || ''.'' || v_table_name || '' ADD PRIMARY KEY  ('' || v_version_key_column || '')'';
        IF v_table_unique_constraint IS NULL THEN
            EXECUTE ''DROP INDEX '' || v_schema_name || ''.'' || v_table_unqiue_index;
        ELSE
            EXECUTE ''ALTER TABLE '' || v_schema_name || ''.'' || v_table_name || '' DROP CONSTRAINT  '' || v_table_unique_constraint;
        END IF;
        EXECUTE ''ALTER TABLE '' || v_schema_name || ''.'' || v_table_name || '' ADD UNIQUE('' || v_table_primary_key || '')'';
    END LOOP;
END;
$PATCH$
'
);

