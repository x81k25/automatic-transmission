# Automatic Transmission

A personal media automation system that ingests torrent files via RSS feeds, downloads them through transmission-daemon, and organizes the content into a proper media library structure compatible with Plex and other media server applications.

## Overview

Automatic Transmission provides an end-to-end pipeline for downloading and organizing media content:

1. Ingests torrent files from RSS feeds
2. Downloads media through transmission-daemon
3. Parses and categorizes the media files
4. Collects metadata for organization
5. Filters media based on configured parameters
6. Initiates and monitors downloads
7. Transfers completed downloads to appropriate library locations
8. Performs cleanup operations

The system supports three media types:
- Movies
- TV Shows
- TV Seasons

## Prerequisites

- Python 3.12
- PostgreSQL database
- Transmission daemon running on the local network

Repository Structure

```
automatic-transmission/
├── config/                   # Configuration files
│   ├── filter-parameters.json
│   └── string-special-conditions.yaml
├── sql/                      # SQL scripts for database setup
├── src/                      # source code
│   ├── core/                 # core pipeline modules
│   │   ├── __init__.py
│   │   ├── 01_rss_ingest.py
│   │   ├── 02_collect.py
│   │   ├── 03_parse.py
│   │   ├── 04_metadata_collection.py
│   │   ├── 05_filter.py
│   │   ├── 06_initiate.py
│   │   ├── 07_download_check.py
│   │   ├── 08_transfer.py
│   │   └── 09_cleanup.py
│   └── utils/                # utility modules
│       ├── __init__.py
│       ├── local_file_operations.py
│       ├── parse_element.py
│       ├── rpcf.py
│       └── sqlf.py
├── test/                     # test scripts and resources
├── venv/                     # virtual environment (gitignored)
├── .env                      # environment variables
├── .gitignore
├── changelog.md              # contains not all major/minor version changes
├── main.py                   # main execution script
├── pytest.ini
├── readme.md                 # this file
└── requirements.txt          # python dependencies
```

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/automatic-transmission.git
   cd automatic-transmission
   ```

2. Install the required Python dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Set up the PostgreSQL database using the scripts in the `/sql` directory. The PostgreSQL scripts below contain placeholder values denoted by `<placeholder>` that will need to filled in with your specifications.
      
   ```
   psql -U your_username -d your_database -f sql/instantiate-schema.sql
   psql -U your_username -d your_database -f sql/back-up-create.sql
   psql -U your_username -d your_database -f sql/back-up-push.sql
   ```

4. Configure your environment variables by copying and filling out the `.env` template:
   ```
   cp templates/.env .env
   ```

## Configuration

A `.env` will need to create with all of the following parameters for your specific configuration. My configuration is currently set-up to with with YTS for movies and episodefeed for TV.

```
# Transmission credentials
SERVER_IP='your_server_ip'
TRANSMISSION_USERNAME='your_transmission_username'
TRANSMISSION_PASSWORD='your_transmission_password'

# PostgreSQL credentials
PG_ENDPOINT='your_postgres_endpoint'
PG_PORT='your_postgres_port'
PG_DATABASE='your_database_name'
PG_SCHEMA='your_schema_name'
PG_USERNAME='your_postgres_username'
PG_PASSWORD='your_postgres_password'

# Media metadata API credentials
OMDB_BASE_URL='https://www.omdbapi.com/'
OMDB_API_KEY='your_omdb_api_key'

# RSS feed URLs
# Generate movie RSS feed from: https://yts.torrentbay.st/rss-guide
MOVIE_RSS_URL='your_movie_rss_url'
# Generate TV show RSS feed from: https://showrss.info/
TV_SHOW_RSS_URL='your_tv_show_rss_url'

# Directory paths
DOWNLOAD_DIR='path_to_download_directory'
MOVIE_DIR='path_to_movie_library'
TV_SHOW_DIR='path_to_tv_show_library'
```

Additional configuration files:
- `config/filter-parameters.json`: Configure media filtering parameters
- `config/string-special-conditions.yaml`: Define special string handling conditions for edge case filenames

## Usage

Execute the pipelines using the command line interface:

### For movies:
```
python main.py movie_pipeline
```

### For TV shows:
```
python main.py tv_show_pipeline
```

### For TV seasons:
```
python main.py tv_season_pipeline
```

### Debug mode
Add the `--debug` flag to enable verbose logging:
```
python main.py movie_pipeline --debug
```

## Core Modules

The application follows a sequential pipeline process:

1. `01_rss_ingest.py`: Ingests torrent files from RSS feeds
2. `02_collect.py`: Collects media information
3. `03_parse.py`: Parses media details
4. `04_metadata_collection.py`: Retrieves and stores metadata for the media
5. `05_filter.py`: Filters media based on configured parameters
6. `06_initiate.py`: Initiates the download process
7. `07_download_check.py`: Monitors download progress
8. `08_transfer.py`: Transfers completed downloads to the media library
9. `09_cleanup.py`: Performs cleanup operations

## Utility Modules

Various utility modules in the `utils` directory provide supporting functions:
- `local_file_operations.py`: Handles local file operations
- `parse_element.py`: Parses XML/RSS elements
- `rpcf.py`: Remote procedure calls
- `sql.py`: Database operations

## License

This project is licensed under the MIT License