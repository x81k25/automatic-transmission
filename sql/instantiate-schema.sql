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
    imdb_id VARCHAR(10) CHECK (imdb_id ~ '^tt[0-9]{7,8}$'),
    tmdb_id INTEGER CHECK (tmdb_id > 0),
    genre VARCHAR(20)[],
    language VARCHAR(20)[],
    rt_score INTEGER CHECK (rt_score IS NULL OR (rt_score BETWEEN 0 AND 100)),
    metascore INTEGER CHECK (metascore IS NULL OR (metascore BETWEEN 0 AND 100)),
    imdb_rating DECIMAL(3,1) CHECK (imdb_rating IS NULL OR (imdb_rating BETWEEN 0 AND 100)),
    imdb_votes INTEGER,
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

-- created_on trigger
DROP TRIGGER IF EXISTS set_created_at_column ON media;

CREATE OR REPLACE FUNCTION set_created_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.created_at = CURRENT_TIMESTAMP AT TIME ZONE 'UTC';
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER set_media_created_at
    BEFORE INSERT ON media
    FOR EACH ROW
    EXECUTE FUNCTION set_created_at_column();

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
COMMENT ON COLUMN media.imdb_id IS 'IMDB identifier for media item';
COMMENT ON COLUMN media.tmdb_id IS 'identifier for themoviedb.org API';
COMMENT ON COLUMN media.genre IS 'array of genres associated with the movie';
COMMENT ON COLUMN media.language IS 'array of languages available encoded in ISO 639 format';
COMMENT ON COLUMN media.rt_score IS 'Rotten Tomatoes score';
COMMENT ON COLUMN media.metascore IS 'MetaCritic score';
COMMENT ON COLUMN media.imdb_rating IS 'IMDB rating out of 10';
COMMENT ON COLUMN media.imdb_votes IS 'number of votes on IMDB';
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