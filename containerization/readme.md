# Containerization

Docker containerization for the automatic-transmission pipeline. All containers run as UID:1005 GID:1001.

## Prerequisites

- Docker and Docker Compose installed
- `.env` file in project root with all required environment variables

## File Structure

```
containerization/
├── dockerfile.00_base
├── dockerfile.01_rss_ingest
└── docker-compose.yml
```

## Build Commands

### Base Image
```bash
# Build base image
docker build -f containerization/dockerfile.00_base -t at-base:latest .

# Or via docker-compose
docker-compose -f containerization/docker-compose.yml --profile build-only up --build base
```

### RSS Ingest Image
```bash
# Build RSS ingest image
docker build -f containerization/dockerfile.01_rss_ingest -t at-rss-ingest:latest .

# Or via docker-compose
docker-compose -f containerization/docker-compose.yml build rss-ingest
```

## Run Commands

### RSS Ingest Pipeline
```bash
# Run RSS ingest (foreground, exits after completion)
docker-compose -f containerization/docker-compose.yml run --rm rss-ingest

# Or direct docker run
docker run --rm --env-file .env at-rss-ingest:latest
```

## Testing

```bash
# Test base image build
docker build -f containerization/dockerfile.00_base -t at-base:test .

# Test RSS ingest with debug logging
docker-compose -f containerization/docker-compose.yml run --rm -e LOG_LEVEL=DEBUG rss-ingest

# Test with shell access
docker run --rm -it --env-file .env --entrypoint /bin/bash at-rss-ingest:latest
```