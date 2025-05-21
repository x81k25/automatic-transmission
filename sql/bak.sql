DO $$
DECLARE
    table_record RECORD;
    current_date_str TEXT;
    source_schema TEXT := 'atp';
    target_schema TEXT := 'bak';
    new_table_name TEXT;
BEGIN
    -- Get current date in YYYYMMDD format
    current_date_str := TO_CHAR(CURRENT_DATE, 'YYYYMMDD');

    -- Create the target schema if it doesn't exist
    EXECUTE 'CREATE SCHEMA IF NOT EXISTS ' || target_schema;

    -- Loop through all tables in the source schema
    FOR table_record IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = source_schema
        AND table_type = 'BASE TABLE'
    LOOP
        -- Create the new table name with format
        new_table_name := source_schema || '_' || table_record.table_name || '_' || current_date_str;

        -- Create a copy of the table in the target schema
        EXECUTE 'CREATE TABLE ' || target_schema || '.' || new_table_name || ' AS
                 SELECT * FROM ' || source_schema || '.' || table_record.table_name;

        RAISE NOTICE 'Created backup: %.% from %.%',
                     target_schema, new_table_name,
                     source_schema, table_record.table_name;
    END LOOP;

    RAISE NOTICE 'Backup complete. All tables from schema "%" have been copied to schema "%"',
                 source_schema, target_schema;
END
$$;