-- replace '<schema-name>' with the source schema name
DO $$
BEGIN
    -- Create a temporary table to store our configuration
    CREATE TEMP TABLE IF NOT EXISTS config(
        source_schema text PRIMARY KEY
    );

    -- Insert or update the source schema name
    INSERT INTO config(source_schema) VALUES('<schema-name>')
    ON CONFLICT (source_schema) DO UPDATE SET source_schema = '<schema-name>';
END $$;

-- Step 0: Complete removal of bak schema and contents
DO $$
BEGIN
    -- Terminate all connections to the bak schema
    EXECUTE (
        SELECT COALESCE(
            string_agg(
                'SELECT pg_terminate_backend(' || pid || ');',
                ' '
            ),
            'SELECT 1;' -- Dummy query when no connections exist
        )
        FROM pg_stat_activity
        WHERE datname = current_database()
        AND current_schema = 'bak'
    );
END $$;

-- Drop schema with CASCADE to ensure complete removal
DROP SCHEMA IF EXISTS bak CASCADE;

-- Rest of the script remains the same as it was working correctly
-- Step 1: Create the new schema
CREATE SCHEMA IF NOT EXISTS bak;

-- Step 2: Get list of all tables from source schema and handle type conversion
DO $$
DECLARE
    curr_table text;
    create_table_sql text;
    source_schema text;
    column_info record;
    column_type text;
BEGIN
    -- Get the source schema name from config
    SELECT c.source_schema INTO source_schema FROM config c;

    -- Loop through all tables in source schema
    FOR curr_table IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = source_schema
    LOOP
        -- First create table with a dummy column (PostgreSQL doesn't allow empty tables)
        EXECUTE 'CREATE TABLE bak.' || curr_table || ' (temp_col int)';

        -- Drop the temporary column
        EXECUTE 'ALTER TABLE bak.' || curr_table || ' DROP COLUMN temp_col';

        -- Get column information
        FOR column_info IN
            SELECT
                column_name,
                data_type,
                udt_name,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                udt_schema
            FROM information_schema.columns
            WHERE table_schema = source_schema
            AND table_name = curr_table
        LOOP
            -- Determine the column type
            IF column_info.data_type = 'ARRAY' THEN
                -- Handle array types by getting the base type from udt_name
                column_type := substring(column_info.udt_name from 2) || '[]';
            ELSIF column_info.data_type = 'USER-DEFINED' THEN
                -- Convert enum types to text
                column_type := 'text';
            ELSE
                column_type := column_info.data_type;
            END IF;

            -- Add the column with appropriate type
            EXECUTE 'ALTER TABLE bak.' || curr_table ||
                    ' ADD COLUMN ' || quote_ident(column_info.column_name) || ' ' || column_type;
        END LOOP;

        -- Copy data with type conversion for enums
        EXECUTE 'INSERT INTO bak.' || curr_table ||
                ' SELECT ' || (
                    SELECT string_agg(
                        CASE
                            WHEN data_type = 'USER-DEFINED' THEN quote_ident(column_name) || '::text'
                            ELSE quote_ident(column_name)
                        END,
                        ','
                    )
                    FROM information_schema.columns
                    WHERE table_schema = source_schema
                    AND table_name = curr_table
                ) ||
                ' FROM ' || source_schema || '.' || curr_table;

        -- Log the completion of each table
        RAISE NOTICE 'Copied table: %', curr_table;
    END LOOP;
END $$;

-- Step 3: Verify the copy
DO $$
DECLARE
    curr_table text;
    source_count bigint;
    bak_count bigint;
    source_schema text;
BEGIN
    -- Get the source schema name from config
    SELECT c.source_schema INTO source_schema FROM config c;

    -- Loop through all tables and compare row counts
    FOR curr_table IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = source_schema
    LOOP
        EXECUTE 'SELECT COUNT(*) FROM ' || source_schema || '.' || curr_table INTO source_count;
        EXECUTE 'SELECT COUNT(*) FROM bak.' || curr_table INTO bak_count;

        IF source_count = bak_count THEN
            RAISE NOTICE 'Table % verified: % rows', curr_table, source_count;
        ELSE
            RAISE WARNING 'Table % mismatch: source=%, bak=%',
                         curr_table, source_count, bak_count;
        END IF;
    END LOOP;
END $$;

-- Clean up
DROP TABLE IF EXISTS config;