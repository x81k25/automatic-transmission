-- compare hashes ------------------------------------------------------------------------------------------------------
WITH input_hashes (hash) AS (
    SELECT unnest(ARRAY[
        'hash1',
        'hash2',
        'hash3',
        'hash4'
    ]::text[]) as hash
)
SELECT input_hashes.hash
FROM input_hashes
LEFT JOIN test.movies ON test.movies.hash = input_hashes.hash
WHERE test.movies.hash IS NULL;

-- combine 2 dfs --------------------------------------------------------------------------------------------------------

UPDATE target_table t
SET
    column1 = v.new_column1,
    column2 = v.new_column2,
    column3 = v.new_column3
FROM (VALUES
    (1, 'new_value1', 100, 'data1'),  -- id, column1, column2, column3
    (2, 'new_value2', 200, 'data2'),
    (3, 'new_value3', 300, 'data3')
    -- ... more rows
) AS v(id, new_column1, new_column2, new_column3)
WHERE t.id = v.id;

-- create and edit media user ------------------------------------------------------------------------------------------

-- Create the restricted user
CREATE USER x81_media WITH PASSWORD 'dgJZmTsDL4RcCfbA';

-- Grant schema usage
GRANT USAGE ON SCHEMA test TO x81_media;

-- Grant specific table permissions
GRANT SELECT, INSERT, UPDATE ON test.movies TO x81_media;

-- Grant sequence permissions if you have any serial columns
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA test TO x81_media;

------------------------------------------------------------------------------------------------------------------------