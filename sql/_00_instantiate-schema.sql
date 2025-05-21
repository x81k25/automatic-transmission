--------------------------------------------------------------------------------
-- schema config
--------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS atp;
SET search_path TO atp;

--------------------------------------------------------------------------------
-- enums
--------------------------------------------------------------------------------

-- Create the media_type enum type
DO $$
BEGIN
    DROP TYPE IF EXISTS media_type;
    CREATE TYPE media_type AS ENUM (
        'movie',
        'tv_show',
        'tv_season'
    );
EXCEPTION
    WHEN others THEN NULL;
END $$;

-- Create the pipeline_status enum type
DO $$
BEGIN
    DROP TYPE IF EXISTS pipeline_status;
    CREATE TYPE pipeline_status AS ENUM (
        'ingested',
        'paused',
        'parsed',
        'metadata_collected',
        'rejected',
        'queued',
        'downloading',
        'downloaded',
        'transferred',
        'complete'
    );
EXCEPTION
    WHEN others THEN NULL;
END $$;

-- Create the rejection_status enum type
DO $$
BEGIN
    DROP TYPE IF EXISTS rejection_status;
    CREATE TYPE rejection_status AS ENUM (
        'unfiltered',
        'accepted',
        'rejected',
        'override'
    );
EXCEPTION
    WHEN others THEN NULL;
END $$;

-- create rss_source enum type
DO $$
BEGIN
    DROP TYPE IF EXISTS rss_source;
    CREATE TYPE rss_source AS ENUM (
        'yts.mx',
        'episodefeed.com'
    );
EXCEPTION
    WHEN others THEN NULL;
END $$;

--------------------------------------------------------------------------------
-- table creation statement
--------------------------------------------------------------------------------

-- create the primary media table
DROP TABLE IF EXISTS media;

CREATE TABLE media (
    -- identifier column
    hash CHAR(40) PRIMARY KEY CHECK (hash ~ '^[a-f0-9]+$' AND length(hash) = 40),
    -- media information
    media_type media_type NOT NULL,
    media_title VARCHAR(255),
    season INTEGER,
    episode INTEGER,
    release_year INTEGER CHECK (release_year BETWEEN 1850 AND 2100),
    -- pipeline status information
    pipeline_status pipeline_status NOT NULL DEFAULT 'ingested',
    error_status BOOLEAN DEFAULT FALSE NOT NULL,
    error_condition TEXT,
    rejection_status rejection_status NOT NULL DEFAULT 'unfiltered',
    rejection_reason TEXT,
    -- path information
    parent_path TEXT,
    target_path TEXT,
    -- download information
    original_title TEXT NOT NULL,
    original_path TEXT,
    original_link TEXT,
    rss_source rss_source,
    uploader VARCHAR(25),
    -- metadata pertaining to the media item
    -- - other identifiers
    imdb_id VARCHAR(10) CHECK (imdb_id ~ '^tt[0-9]{7,8}$'),
    tmdb_id INTEGER CHECK (tmdb_id > 0),
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
    -- metadata pertaining to the video file
    resolution VARCHAR(10),
    video_codec VARCHAR(10),
    upload_type VARCHAR(10),
    audio_codec VARCHAR(10),
    -- timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') NOT NULL
);

--------------------------------------------------------------------------------
-- additional indices
--------------------------------------------------------------------------------

-- create additional indexes
CREATE INDEX IF NOT EXISTS idx_media_imdb_id ON media(imdb_id);
CREATE INDEX IF NOT EXISTS idx_media_pipeline_status ON media(pipeline_status);

--------------------------------------------------------------------------------
-- triggers
--------------------------------------------------------------------------------

-- updated_at trigger
DROP TRIGGER IF EXISTS update_media_updated_at ON media;

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP AT TIME ZONE 'UTC';
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_media_updated_at
    BEFORE UPDATE ON media
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ingestion clear trigger
DROP TRIGGER IF EXISTS reset_fields_on_ingestion ON media;

CREATE OR REPLACE FUNCTION reset_on_ingestion()
RETURNS TRIGGER AS $$
BEGIN
    IF (NEW.pipeline_status = 'ingested' AND OLD.pipeline_status != 'ingested') THEN
        IF (OLD.rejection_status != 'override') THEN
            NEW.rejection_status = 'unfiltered';
        END IF;
        NEW.rejection_reason = NULL;
        NEW.error_status = FALSE;
        NEW.error_condition = NULL;
    END IF;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER reset_fields_on_ingestion
    BEFORE UPDATE ON media
    FOR EACH ROW
    EXECUTE FUNCTION reset_on_ingestion();

-- error_condition clear trigger
DROP TRIGGER IF EXISTS clear_error_condition ON media;

CREATE OR REPLACE FUNCTION reset_error_condition()
RETURNS TRIGGER AS $$
BEGIN
    IF (NEW.error_status = FALSE) THEN
        NEW.error_condition = NULL;
    END IF;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER clear_error_condition
    BEFORE UPDATE ON media
    FOR EACH ROW
    EXECUTE FUNCTION reset_error_condition();

-- rejection_status clear trigger
DROP TRIGGER IF EXISTS clear_rejection_reason ON media;

CREATE OR REPLACE FUNCTION reset_rejection_reason()
RETURNS TRIGGER AS $$
BEGIN
    IF (NEW.rejection_status != 'rejected') THEN
        NEW.rejection_reason = NULL;
    END IF;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER clear_rejection_reason
    BEFORE UPDATE ON media
    FOR EACH ROW
    EXECUTE FUNCTION reset_rejection_reason();

--------------------------------------------------------------------------------
-- comments
--------------------------------------------------------------------------------

-- add table comment
COMMENT ON TABLE media IS 'stores media data for movies, tv shows, and tv seasons';

-- identifier column
COMMENT ON COLUMN media.hash IS 'primary key; unique identifier; and primary element for interaction with transmission';
-- media information
COMMENT ON COLUMN media.media_type IS 'either movie, tv_shows, or tv_season';
COMMENT ON COLUMN media.media_title IS 'either movie or tv show title';
COMMENT ON COLUMN media.season IS 'media season if tv show or tv season; null for movies';
COMMENT ON COLUMN media.episode IS 'episode number within season for tv show, otherwise null';
COMMENT ON COLUMN media.release_year IS 'year the movie was released or the year of the first season of a tv show';
-- pipeline status information
COMMENT ON COLUMN media.pipeline_status IS 'status within the automatic transmission pipeline';
COMMENT ON COLUMN media.error_status IS 'boolean value documenting errors occurring during pipeline';
COMMENT ON COLUMN media.error_condition IS 'details on error status';
COMMENT ON COLUMN media.rejection_status IS 'rejection status based on filters within filter-parameters.json';
COMMENT ON COLUMN media.rejection_reason IS 'details on which filter flags were tagged, if rejection was caused';
-- path information
COMMENT ON COLUMN media.parent_path IS 'parent dir of media library location for item';
COMMENT ON COLUMN media.target_path IS 'file or dir path for media library location';
-- download information
COMMENT ON COLUMN media.original_title IS 'raw item title string value; used for parsing other field values';
COMMENT ON COLUMN media.original_path IS 'original path of item within media-cache';
COMMENT ON COLUMN media.original_link IS 'may contain either the direct download link or the magnet link';
COMMENT ON COLUMN media.rss_source IS 'source of rss feed for item ingestion, if any';
COMMENT ON COLUMN media.uploader IS 'uploading entity of the media item';
-- metadata pertaining to the media item
-- - other id fields
COMMENT ON COLUMN media.imdb_id IS 'from TMDB; IMDB identifier for media item';
COMMENT ON COLUMN media.tmdb_id IS 'from TMDB; identifier for themoviedb.org API';
-- - quantitative details
COMMENT ON COLUMN media.budget IS 'from TMDB; production budget of media item';
COMMENT ON COLUMN media.revenue IS 'from TMDB; current revenue of media item';
COMMENT ON COLUMN media.runtime IS 'from TMDB; runtime in minutes of media item';
-- - country and production information
COMMENT ON COLUMN media.origin_country IS 'from TMDB; primary country of production in iso_3166_1 format';
COMMENT ON COLUMN media.production_companies IS 'from TMDB; array of production companies';
COMMENT ON COLUMN media.production_countries IS 'from TMDB; array of countries where media item was produced in iso_3166_1 format';
COMMENT ON COLUMN media.production_status IS 'from TMDB; current production status of media item';
-- - language information
COMMENT ON COLUMN media.original_language IS 'from TMDB; primary language of media item in ISO 639 format';
COMMENT ON COLUMN media.spoken_languages IS 'from TMDB; array of languages available encoded in ISO 639 format';
-- - other string fields
COMMENT ON COLUMN media.genre IS 'from TMDB; array of genres associated with the movie';
COMMENT ON COLUMN media.original_media_title IS 'from TMDB; original title of media item';
-- - long string fields
COMMENT ON COLUMN media.tagline IS 'from TMDB; tagline for the media item';
COMMENT ON COLUMN media.overview IS 'from TMDB; brief plot synopsis of media item';
-- - ratings info
COMMENT ON COLUMN media.tmdb_rating IS 'from TMDB; rating submitted by TMDB users out of 10';
COMMENT ON COLUMN media.tmdb_votes IS 'from TMDB; number of ratings by TMDB users';
COMMENT ON COLUMN media.rt_score IS 'from OMDb; Rotten Tomatoes score out of 100';
COMMENT ON COLUMN media.metascore IS 'from OMDb; MetaCritic score out of 100';
COMMENT ON COLUMN media.imdb_rating IS 'from OMDb; IMDB rating out of 100';
COMMENT ON COLUMN media.imdb_votes IS 'from OMDb; number of votes on IMDB';
-- metadata pertaining to the video file
COMMENT ON COLUMN media.resolution IS 'video resolution';
COMMENT ON COLUMN media.video_codec IS 'video compression codec';
COMMENT ON COLUMN media.audio_codec IS 'audio codec';
COMMENT ON COLUMN media.upload_type IS 'uploading type indicating source of upload';
-- timestamps
COMMENT ON COLUMN media.created_at IS 'timestamp for initial database creation of item';
COMMENT ON COLUMN media.updated_at IS 'timestamp of last database alteration of item';

--------------------------------------------------------------------------------
-- end of instantiate_schema.sql
--------------------------------------------------------------------------------