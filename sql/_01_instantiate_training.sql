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
    -- metadata pertaining to the media item
    -- - quantitative details
    budget BIGINT CHECK (budget >= 0),
    revenue BIGINT CHECK (revenue >= 0),
    runtime INTEGER CHECK (runtime >= 0),
    -- - country and production information
    origin_country CHAR(2)[],
    production_companies VARCHAR(255)[],
    production_countries CHAR(2)[],
    production_status VARCHAR(25),
    -- - language information
    original_language CHAR(2),
    spoken_languages CHAR(2)[],
    -- - other string fields
    genre VARCHAR(20)[],
    original_media_title VARCHAR(255),
    -- - long string fields
    tagline VARCHAR(255),
    overview TEXT,
    -- - ratings info
    tmdb_rating DECIMAL(5,3) CHECK (tmdb_rating BETWEEN 0 AND 10),
    tmdb_votes INTEGER CHECK (tmdb_votes >= 0),
    rt_score INTEGER CHECK (rt_score IS NULL OR (rt_score BETWEEN 0 AND 100)),
    metascore INTEGER CHECK (metascore IS NULL OR (metascore BETWEEN 0 AND 100)),
    imdb_rating DECIMAL(4,1) CHECK (imdb_rating IS NULL OR (imdb_rating BETWEEN 0 AND 100)),
    imdb_votes INTEGER CHECK (imdb_votes >= 0),
    -- flag columns
    human_labeled BOOLEAN NOT NULL DEFAULT FALSE,
    anomalous BOOLEAN NOT NULL DEFAULT FALSE,
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
COMMENT ON COLUMN training.imdb_id IS 'from TMDB; IMDB identifier for media item, and the primary key for this column';
COMMENT ON COLUMN training.tmdb_id IS 'from TMDB; identifier for themoviedb.org API';
-- label columns
COMMENT ON COLUMN training.label IS 'training label enum value for model ingestion';
-- media identifying information
COMMENT ON COLUMN training.media_type IS 'either movie, tv_shows, or tv_season';
COMMENT ON COLUMN training.media_title IS 'either movie or tv show title';
COMMENT ON COLUMN training.season IS 'media season if tv show or tv season; null for movies';
COMMENT ON COLUMN training.episode IS 'episode number within season for tv show, otherwise null';
COMMENT ON COLUMN training.release_year IS 'year the movie was released or the year of the first season of a tv show';
-- metadata pertaining to the media item
-- - quantitative details
COMMENT ON COLUMN training.budget IS 'from TMDB; production budget of media item';
COMMENT ON COLUMN training.revenue IS 'from TMDB; current revenue of media item';
COMMENT ON COLUMN training.runtime IS 'from TMDB; runtime in minutes of media item';
-- - country and production information
COMMENT ON COLUMN training.origin_country IS 'from TMDB; primary country of production in iso_3166_1 format';
COMMENT ON COLUMN training.production_companies IS 'from TMDB; array of production companies';
COMMENT ON COLUMN training.production_countries IS 'from TMDB; array of countries where media item was produced in iso_3166_1 format';
COMMENT ON COLUMN training.production_status IS 'from TMDB; current production status of media item';
-- - language information
COMMENT ON COLUMN training.original_language IS 'from TMDB; primary language of media item in ISO 639 format';
COMMENT ON COLUMN training.spoken_languages IS 'from TMDB; array of languages available encoded in ISO 639 format';
-- - other string fields
COMMENT ON COLUMN training.genre IS 'from TMDB; array of genres associated with the movie';
COMMENT ON COLUMN training.original_media_title IS 'from TMDB; original title of media item';
-- - long string fields
COMMENT ON COLUMN training.tagline IS 'from TMDB; tagline for the media item';
COMMENT ON COLUMN training.overview IS 'from TMDB; brief plot synopsis of media item';
-- - ratings info
COMMENT ON COLUMN training.tmdb_rating IS 'from TMDB; rating submitted by TMDB users out of 10';
COMMENT ON COLUMN training.tmdb_votes IS 'from TMDB; number of ratings by TMDB users';
COMMENT ON COLUMN training.rt_score IS 'Rotten Tomatoes score';
COMMENT ON COLUMN training.metascore IS 'MetaCritic score';
COMMENT ON COLUMN training.imdb_rating IS 'IMDB rating out of 100';
COMMENT ON COLUMN training.imdb_votes IS 'number of votes on IMDB';
-- flag columns
COMMENT ON COLUMN training.human_labeled IS 'flag value that indicates a user change of the label value';
COMMENT ON COLUMN training.anomalous IS 'user set flag for media items frequently appear as false postives or false negatvies in model results, but have been verified to be correct';
-- timestamps
COMMENT ON COLUMN training.created_at IS 'timestamp for initial database creation of item';
COMMENT ON COLUMN training.updated_at IS 'timestamp of last database alteration of item';

--------------------------------------------------------------------------------
-- end of instantiate_schema.sql
--------------------------------------------------------------------------------
