# automatic transmission

A personal media automation system that ingests torrent files via RSS feeds, downloads them through transmission-daemon, and organizes the content into a proper media library structure compatible with Plex and other media server applications.

## Overview

automatic transmission provides an end-to-end pipeline for downloading and organizing media content:

1. ingests torrent files from RSS feeds
2. collects media information from existing transmission downloads
3. parses and categorizes the media files
4. collects metadata from multiple APIs for organization and filtering
5. filters media based on configured parameters and prediction scores
6. initiates and monitors downloads
7. transfers completed downloads to appropriate library locations
8. performs cleanup operations
9. handles hung downloads and maintains the database

The system supports three media types:
- movies
- TV shows (individual episodes)
- TV seasons (complete seasons)

## Prerequisites

- Python 3.12
- PostgreSQL database
- Transmission daemon running on the local network
- API credentials for metadata services (TMDB, OMDb)
- Connection to the reel-driver API for media recommendation scoring (optional)

## Repository Structure

```
automatic-transmission/
├── config/                   # Configuration files
│   ├── filter-parameters.yaml
│   └── string-special-conditions.yaml
├── sql/                      # SQL scripts for database setup
├── src/                      # source code
│   ├── core/                 # core pipeline modules
│   │   ├── __init__.py
│   │   ├── _01_rss_ingest.py
│   │   ├── _02_collect.py
│   │   ├── _03_parse.py
│   │   ├── _04_metadata_collection.py
│   │   ├── _05_filter.py
│   │   ├── _06_initiate.py
│   │   ├── _07_download_check.py
│   │   ├── _08_transfer.py
│   │   └── _09_cleanup.py
│   ├── data_models/          # data models for validation/transformation
│   │   ├── __init__.py
│   │   └── media_data_frame.py
│   ├── error_handling/       # ad-hoc pipeline maintenance  
│   │   └── error_handling.py
│   └── utils/                # utility modules
│       ├── __init__.py
│       ├── local_file_operations.py
│       ├── parse_element.py
│       ├── rpcf.py
│       └── sqlf.py
├── test/                     # test scripts and resources
├── .venv/                    # virtual environment (gitignored)
├── .env                      # environment variables
├── .gitignore
├── changelog.md              # contains all major/minor version changes
├── main.py                   # main execution script
├── pytest.ini
├── readme.md                 # this file
└── requirements.txt          # python dependencies
```

## Core Data Model

The project centers around the `MediaDataFrame` class, which serves as a rigid schema-enforcing wrapper around a polars DataFrame. This class:

- Mirrors the structure of the consolidated PostgreSQL `media` table
- Performs input validation before database operations
- Handles timestamp conversion and type enforcement
- Standardizes data operations throughout the pipeline

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/automatic-transmission.git
   cd automatic-transmission
   ```

2. Create a virtual environment and activate it:
   ```
   python -m venv .venv
   # On Windows
   .venv\Scripts\activate
   # On Unix or MacOS
   source .venv/bin/activate
   ```

3. Install dependencies using uv (recommended):
   ```
   # Install uv if you don't have it
   pip install uv
   
   # Install dependencies from requirements.txt
   uv pip install -r requirements.txt
   ```

   Alternatively, you can use pip:
   ```
   pip install -r requirements.txt
   ```

4. Adding new packages:
   ```
   # Add new package to requirements.in
   echo "new-package" >> requirements.in
   
   # Compile requirements.in to requirements.txt
   uv pip compile requirements.in -o requirements.txt
   
   # Install the updated requirements
   uv pip install -r requirements.txt
   ```

5. Set up the PostgreSQL database using the scripts in the `/sql` directory:
   ```
   psql -U your_username -d your_database -f sql/instantiate-schema.sql
   ```

6. Configure your environment variables by creating a `.env` file with the required parameters (see Configuration section below).

7. Optional: Set up connection to the reel-driver API if you want to use ML-based media recommendation scoring.

## Configuration

Create a `.env` file with the following parameters customized for your setup:

```
# Transmission credentials
TRANSMISSION_USERNAME=your_transmission_username
TRANSMISSION_PASSWORD=your_transmission_password
TRANSMISSION_PORT=your_transmission_port

# PostgreSQL credentials
PG_ENDPOINT=your_postgres_endpoint
PG_PORT=your_postgres_port
PG_DATABASE=your_database_name
PG_USERNAME=your_postgres_username
PG_PASSWORD=your_postgres_password
PG_SCHEMA=your_schema_name

# Media metadata API credentials (TMDB and OMDb)
# Movie APIs
MOVIE_SEARCH_API_BASE_URL=https://api.themoviedb.org/3/search/movie
MOVIE_DETAILS_API_BASE_URL=https://api.themoviedb.org/3/movie
MOVIE_RATINGS_API_BASE_URL=https://www.omdbapi.com/
MOVIE_SEARCH_API_KEY=your_tmdb_api_key
MOVIE_DETAILS_API_KEY=your_tmdb_api_key
MOVIE_RATINGS_API_KEY=your_omdb_api_key

# TV Show APIs
TV_SEARCH_API_BASE_URL=https://api.themoviedb.org/3/search/tv
TV_DETAILS_API_BASE_URL=https://api.themoviedb.org/3/tv
TV_RATINGS_API_BASE_URL=https://www.omdbapi.com/
TV_SEARCH_API_KEY=your_tmdb_api_key
TV_DETAILS_API_KEY=your_tmdb_api_key
TV_RATINGS_API_KEY=your_omdb_api_key

# Reel-driver API for ML-based recommendation scoring
REEL_DRIVER_HOST=your_reel_driver_host
REEL_DRIVER_POST=your_reel_driver_port
REEL_DRIVER_THRESHOLD=0.75

# RSS feeds configuration
# RSS_SOURCES and RSS_URLS must be in same order, comma-separated
RSS_SOURCES=yts.mx,episodefeed.com
RSS_URLS=your_movie_rss_url,your_tv_show_rss_url

# Directory paths
DOWNLOAD_DIR=path_to_download_directory
MOVIE_DIR=path_to_movie_library
TV_SHOW_DIR=path_to_tv_show_library

# Cleanup delay settings (in seconds)
TRANSFERRED_ITEM_CLEANUP_DELAY=3600
HUNG_ITEM_CLEANUP_DELAY=86400
```

Additional configuration files:
- `config/filter-parameters.yaml`: Configure media filtering parameters
- `config/string-special-conditions.yaml`: Define special string handling conditions for edge case filenames

## Usage

Execute the complete pipeline using the command line interface:

```
python main.py
```

### Debug Mode
Add the `--debug` flag to enable verbose logging:
```
python main.py --debug
```

## Pipeline Process

The application follows a sequential pipeline process:

1. `_01_rss_ingest.py`: Ingests torrent files from configured RSS feeds
2. `_02_collect.py`: Collects media information from existing transmission downloads
3. `_03_parse.py`: Parses media details from filenames and metadata
4. `_04_metadata_collection.py`: Retrieves and stores metadata from TMDB and OMDb APIs
5. `_05_filter.py`: Filters media based on configured parameters and reel-driver API predictions
6. `_06_initiate.py`: Initiates the download process for accepted media
7. `_07_download_check.py`: Monitors download progress and status
8. `_08_transfer.py`: Transfers completed downloads to the appropriate media library locations
9. `_09_cleanup.py`: Performs cleanup operations for transferred media and handles hung downloads

## Error Handling and Maintenance

The `src/error_handling` module provides utilities for ad-hoc pipeline maintenance:

- Recycling items stuck in the downloading state
- Marking items as complete to bypass pipeline stages
- Re-ingesting items for reprocessing
- Rejecting hung downloads
- Re-parsing and reprocessing metadata for specified items

## Utilities

Various utility modules in the `utils` directory provide supporting functions:

- `local_file_operations.py`: Handles file system operations for media transfer
- `parse_element.py`: Parses XML/RSS elements and extracts metadata from filenames
- `rpcf.py`: Handles interaction with the Transmission RPC API
- `sqlf.py`: Manages database operations with PostgreSQL

## Documentation

Additional documentation is available in the project's wiki. Refer to the wiki for more detailed information about the pipeline processes, troubleshooting, and advanced configurations.

## License

This project is licensed under the MIT License.