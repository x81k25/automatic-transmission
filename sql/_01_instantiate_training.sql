--------------------------------------------------------------------------------
-- schema config
--
-- unlike _00_instantiate-schema.sql, this script will not be set to delete
--   or reset any critical items
--------------------------------------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_namespace WHERE nspname = 'atp'
    ) THEN
        RAISE EXCEPTION 'Schema "atp" does not exist';
    END IF;
END $$;

SET search_path TO atp;

--------------------------------------------------------------------------------
-- enums
--------------------------------------------------------------------------------

-- requires the following enums from _00_instantiate-schema.sql
-- - media_type

-- Create the label enum type
DO $$
BEGIN
    DROP TYPE IF EXISTS label_type;
    CREATE TYPE label_type AS ENUM (
        'would_watch',
        'would_not_watch'
    );
EXCEPTION
    WHEN others THEN NULL;
END $$;

--------------------------------------------------------------------------------
-- table creation statement
--------------------------------------------------------------------------------

CREATE TABLE training (
    -- identifier columns
    imdb_id VARCHAR(10) PRIMARY KEY CHECK (imdb_id ~ '^tt[0-9]{7,8}$'),
    tmdb_id INTEGER UNIQUE CHECK (tmdb_id > 0),
    -- label columns
    label label_type NOT NULL,
    -- media identifying information
    media_type media_type NOT NULL,
    media_title VARCHAR(255) NOT NULL,
    season SMALLINT,
    episode SMALLINT,
    release_year SMALLINT CHECK (release_year BETWEEN 1850 AND 2100) NOT NULL,
    -- media metadata
    genre VARCHAR(20)[],
    language VARCHAR(20)[],
    rt_score SMALLINT CHECK (rt_score IS NULL OR (rt_score BETWEEN 0 AND 100)),
    metascore SMALLINT CHECK (metascore IS NULL OR (metascore BETWEEN 0 AND 100)),
    imdb_rating DECIMAL(3,1) CHECK (imdb_rating IS NULL OR (imdb_rating BETWEEN 0 AND 100)),
    imdb_votes INTEGER,
    -- timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') NOT NULL
);

--------------------------------------------------------------------------------
-- additional indices
--------------------------------------------------------------------------------

-- create additional indexes
CREATE INDEX IF NOT EXISTS idx_training_tmdb_id ON training(tmdb_id);

--------------------------------------------------------------------------------
-- triggers
--------------------------------------------------------------------------------

-- Drop existing trigger and function
DROP TRIGGER IF EXISTS trg_training_update_timestamp ON training;
DROP FUNCTION IF EXISTS trg_fn_training_update_timestamp();

-- Create new function
CREATE OR REPLACE FUNCTION trg_fn_training_update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP AT TIME ZONE 'UTC';
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create new trigger
CREATE TRIGGER trg_training_update_timestamp
    BEFORE UPDATE ON training
    FOR EACH ROW
    EXECUTE FUNCTION trg_fn_training_update_timestamp();

--------------------------------------------------------------------------------
-- comments
--------------------------------------------------------------------------------

-- add table comment
COMMENT ON TABLE training IS 'stores training data to be ingested by reel-driver';

-- identifier columns
COMMENT ON COLUMN training.imdb_id IS 'IMDB identifier for media item, and the primary key for this column';
COMMENT ON COLUMN training.tmdb_id IS 'identifier for themoviedb.org API';
-- label columns
COMMENT ON COLUMN training.label IS 'training label enum value for model ingestion';
-- media identifying information
COMMENT ON COLUMN training.media_type IS 'either movie, tv_shows, or tv_season';
COMMENT ON COLUMN training.media_title IS 'either movie or tv show title';
COMMENT ON COLUMN training.season IS 'media season if tv show or tv season; null for movies';
COMMENT ON COLUMN training.episode IS 'episode number within season for tv show, otherwise null';
COMMENT ON COLUMN training.release_year IS 'year the movie was released or the year of the first season of a tv show';
-- media metadata
COMMENT ON COLUMN training.genre IS 'array of genres associated with the movie';
COMMENT ON COLUMN training.language IS 'array of languages available encoded in ISO 639 format';
COMMENT ON COLUMN training.rt_score IS 'Rotten Tomatoes score';
COMMENT ON COLUMN training.metascore IS 'MetaCritic score';
COMMENT ON COLUMN training.imdb_rating IS 'IMDB rating out of 100';
COMMENT ON COLUMN training.imdb_votes IS 'number of votes on IMDB';
-- timestamps
COMMENT ON COLUMN training.created_at IS 'timestamp for initial database creation of item';
COMMENT ON COLUMN training.updated_at IS 'timestamp of last database alteration of item';

--------------------------------------------------------------------------------
-- end of instantiate_schema.sql
--------------------------------------------------------------------------------
