CREATE SCHEMA IF NOT EXISTS <schema_name>;
SET search_path TO <schema_name>;

-- no placeholder fill-ins required after this line

-- create the status enum type
DO $$
BEGIN
    -- Drop type if it exists (this will error if the type is in use)
    DROP TYPE IF EXISTS media_status;

    -- Create the type
    CREATE TYPE media_status AS ENUM (
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
    WHEN others THEN
        -- If there's an error (like the type being in use), we'll ignore it
        NULL;
END $$;

-- create the rejection status enum type
DO $$
BEGIN
    -- Drop type if it exists (this will error if the type is in use)
    DROP TYPE IF EXISTS rejection_status;

    -- Create the type
    CREATE TYPE rejection_status AS ENUM (
        'unfiltered',
        'passed',
        'failed',
        'override'
    );
EXCEPTION
    WHEN others THEN
        -- If there's an error (like the type being in use), we'll ignore it
        NULL;
END $$;

-- Create the movies table
CREATE TABLE IF NOT EXISTS movies (
    hash VARCHAR(255) PRIMARY KEY,
    raw_title TEXT NOT NULL,
    movie_title VARCHAR(255),
    release_year INTEGER,
    status media_status,
    torrent_source TEXT,
    rejection_status rejection_status NOT NULL DEFAULT 'unfiltered',
    rejection_reason VARCHAR(255),
    published_timestamp TIMESTAMP WITH TIME ZONE,
    summary TEXT,
    genre TEXT[], -- Using array for multiple genres
    language TEXT[], -- Using array for multiple languages
    metascore INTEGER,
    rt_score INTEGER,
    imdb_rating DECIMAL(3,1),
    imdb_votes INTEGER,
    imdb_id VARCHAR(20),
    resolution VARCHAR(20),
    video_codec VARCHAR(50),
    upload_type VARCHAR(50),
    audio_codec VARCHAR(50),
    file_name TEXT,
    uploader VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- create additional indexes
CREATE INDEX IF NOT EXISTS idx_movies_imdb_id ON movies(imdb_id);
CREATE INDEX IF NOT EXISTS idx_movies_status ON movies(status);

-- drop automatic update trigger if exists
DROP TRIGGER IF EXISTS update_movies_updated_at ON movies;

-- create function if not exists
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- create trigger
CREATE TRIGGER update_movies_updated_at
    BEFORE UPDATE ON movies
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- add table comment
COMMENT ON TABLE movies IS 'Stores movie metadata and torrent information';

-- Add column comments
COMMENT ON COLUMN movies.hash IS 'primary key - unique identifier for the movie entry';
COMMENT ON COLUMN movies.movie_title IS 'official title of the movie';
COMMENT ON COLUMN movies.torrent_source IS 'may contain either the direct download link or the magnet link';
COMMENT ON COLUMN movies.release_year IS 'year the movie was released';
COMMENT ON COLUMN movies.genre IS 'array of genres associated with the movie';
COMMENT ON COLUMN movies.language IS 'array of languages available';
COMMENT ON COLUMN movies.imdb_rating IS 'IMDB rating out of 10';
COMMENT ON COLUMN movies.imdb_votes IS 'Number of votes on IMDB';

-- Create tv_shows table
CREATE TABLE IF NOT EXISTS tv_shows (
    hash VARCHAR(255) PRIMARY KEY,
    raw_title TEXT NOT NULL,
    tv_show_name VARCHAR(255),
    season INTEGER,
    episode INTEGER,
    status media_status,
    torrent_source TEXT,
    published_timestamp TIMESTAMP WITH TIME ZONE,
    rejection_status rejection_status NOT NULL DEFAULT 'unfiltered',
    rejection_reason TEXT,
    summary TEXT,
    release_year INTEGER,
    genre TEXT[], -- Using array for multiple genres
    language TEXT[], -- Using array for multiple languages
    metascore INTEGER,
    imdb_rating DECIMAL(3,1), -- Using DECIMAL for float32
    imdb_votes INTEGER,
    imdb_id VARCHAR(20),
    resolution VARCHAR(20),
    video_codec VARCHAR(50),
    upload_type VARCHAR(50),
    audio_codec VARCHAR(50),
    uploader VARCHAR(100),
    file_name TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- create additional index
CREATE INDEX IF NOT EXISTS idx_tv_shows_imdb_id ON tv_shows(imdb_id);
CREATE INDEX IF NOT EXISTS idx_tv_shows_status ON tv_shows(status);

-- drop automatic update trigger if exists
DROP TRIGGER IF EXISTS update_tv_shows_updated_at ON tv_shows;

-- create trigger
CREATE TRIGGER update_tv_shows_updated_at
    BEFORE UPDATE ON tv_shows
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Add table comment
COMMENT ON TABLE tv_shows IS 'Stores TV show metadata and torrent information';

-- Add column comments
COMMENT ON COLUMN tv_shows.hash IS 'primary key - unique identifier for the TV show entry';
COMMENT ON COLUMN tv_shows.tv_show_name IS 'name of the TV show';
COMMENT ON COLUMN tv_shows.season IS 'season number';
COMMENT ON COLUMN tv_shows.episode IS 'episode number';
COMMENT ON COLUMN tv_shows.torrent_source IS 'may contain either the direct download link or the magnet link';
COMMENT ON COLUMN tv_shows.imdb_rating IS 'IMDB rating out of 10';
COMMENT ON COLUMN tv_shows.genre IS 'array of genres associated with the show';
COMMENT ON COLUMN tv_shows.language IS 'array of languages available';

-- Create tv_seasons table
CREATE TABLE IF NOT EXISTS tv_seasons (
    hash VARCHAR(255) PRIMARY KEY,
    raw_title TEXT NOT NULL,
    tv_show_name VARCHAR(255),
    season INTEGER,
    status media_status,
    torrent_source TEXT,
    rejection_status rejection_status NOT NULL DEFAULT 'unfiltered',
    rejection_reason TEXT,
    release_year INTEGER,
    genre TEXT[], -- Using array for multiple genres
    language TEXT[], -- Using array for multiple languages
    metascore INTEGER,
    imdb_rating DECIMAL(3,1), -- Using DECIMAL for float32
    imdb_votes INTEGER,
    imdb_id VARCHAR(20),
    resolution VARCHAR(20),
    video_codec VARCHAR(50),
    upload_type VARCHAR(50),
    audio_codec VARCHAR(50),
    uploader VARCHAR(100),
    file_name TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- create additional index
CREATE INDEX IF NOT EXISTS idx_tv_seasons_imdb_id ON tv_seasons(imdb_id);
CREATE INDEX IF NOT EXISTS idx_tv_seasons_status ON tv_seasons(status);

-- drop automatic update trigger if exists
DROP TRIGGER IF EXISTS update_tv_seasons_updated_at ON tv_seasons;

-- create trigger
CREATE TRIGGER update_tv_seasons_updated_at
    BEFORE UPDATE ON tv_seasons
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Add table comment
COMMENT ON TABLE tv_seasons IS 'Stores TV show metadata for full seasons and torrent information';

-- Add column comments
COMMENT ON COLUMN tv_seasons.hash IS 'primary key - unique identifier for the TV show entry';
COMMENT ON COLUMN tv_seasons.tv_show_name IS 'name of the TV show';
COMMENT ON COLUMN tv_seasons.season IS 'season number';
COMMENT ON COLUMN tv_seasons.torrent_source IS 'may contain either the direct download link or the magnet link';
COMMENT ON COLUMN tv_seasons.imdb_rating IS 'IMDB rating out of 10';
COMMENT ON COLUMN tv_seasons.genre IS 'array of genres associated with the show';
COMMENT ON COLUMN tv_seasons.language IS 'array of languages available';
