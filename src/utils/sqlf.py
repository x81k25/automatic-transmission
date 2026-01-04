# standard library imports
import logging
import os
import sys
from typing import List, Optional
import warnings

# third-party imports
from dotenv import load_dotenv
import polars as pl
from sqlalchemy import create_engine, text, Engine, Table, MetaData, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import URL
from sqlalchemy.exc import SAWarning

# internal imports - none needed after MediaDataFrame removal

# ------------------------------------------------------------------------------
# load in environment variables
# ------------------------------------------------------------------------------
load_dotenv(override=True)

pg_username = os.getenv('AT_PGSQL_USERNAME')
pg_password = os.getenv('AT_PGSQL_PASSWORD')
pg_host = os.getenv('AT_PGSQL_ENDPOINT')
pg_port = os.getenv('AT_PGSQL_PORT')
pg_database = os.getenv('AT_PGSQL_DATABASE')
pg_schema = os.getenv('AT_PGSQL_SCHEMA')

# ------------------------------------------------------------------------------
# sql functions to be used by core packages
# ------------------------------------------------------------------------------

def create_db_engine(
    username: Optional[str] = pg_username,
    password: Optional[str] = pg_password,
    host: Optional[str] = pg_host,
    port: Optional[str] = pg_port,
    database: Optional[str] = pg_database,
    schema: Optional[str] = pg_schema
) -> Engine:
    """
    Creates and returns a SQLAlchemy engine for PostgresQL connection with specified schema.
    Uses environment variables if parameters are not provided.

    :param username: Database username (default: AT_PGSQL_USERNAME env var)
    :param password: Database password (default: AT_PGSQL_PASSWORD env var)
    :param host: Database host address (default: AT_PGSQL_ENDPOINT env var)
    :param port: Database port (default: AT_PGSQL_PORT env var)
    :param database: Database name (default: AT_PGSQL_DATABASE env var)
    :param schema: Database schema (default: AT_PGSQL_SCHEMA env var or 'public')
    :return: Configured database engine
    :raises RuntimeError: If connection cannot be established with detailed error message
    :raises ValueError: If required parameters are missing
    """
    required_params = {
        'username': username,
        'password': password,
        'host': host,
        'port': port,
        'database': database,
        'schema': schema
    }

    missing_params = [k for k, v in required_params.items() if not v]
    if missing_params:
        error_msg = f"Missing required database parameters: {', '.join(missing_params)}"
        logging.error(error_msg)
        raise ValueError(error_msg)

    url = URL.create(
        drivername="postgresql+psycopg2",
        username=username,
        password=password,
        host=host,
        port=port,
        database=database
    )

    engine = create_engine(
        url,
        connect_args={'options': f'-c search_path={schema}'},
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800,
        echo=False
    )

    try:
        with engine.connect():
            return engine
    except:
        error_msg = (
            f"Unable to connect to database at {host}:{port}\n"
            f"Connection timed out. Please check:\n"
            f"- The database server is running\n"
            f"- The host and port are correct\n"
            f"- Any firewalls or network settings are blocking the connection"
        )
        logging.error(error_msg)
        sys.exit(1)

# ------------------------------------------------------------------------------
# select statements
# ------------------------------------------------------------------------------

def compare_hashes_to_db(
    hashes: List[str],
    pipeline_status: str = None
):
    """
    Compares hashes in the input list against existing hashes in the database.

    :param hash_list: List of hash strings
    :param engine: SQLAlchemy engine connection
    :return: tuple (new_hashes, existing_hashes) where new_hashes is list of hashes not in the database and existing_hashes is list of hashes that exist in the database
    """
    # assign engine if none provided
    engine = create_db_engine()

    # define pipeline_status condition if exists
    pipeline_status_condition = f"AND media.pipeline_status = {pipeline_status}" if pipeline_status is not None else ""

    try:
        # Query existing hashes from database
        query = text(f"""
            WITH input_hashes (hash) AS (
                SELECT unnest(ARRAY{hashes}::text[]) as hash
            )
            SELECT input_hashes.hash
            FROM input_hashes
            LEFT JOIN media ON media.hash = input_hashes.hash
                AND media.deleted_at IS NULL
            WHERE media.hash IS NULL
            {pipeline_status_condition};
        """)

        # Read existing hashes into a list
        with engine.connect() as conn:
            new_hashes = conn.execute(query).fetchall()
            new_hashes = [hash_tuple[0] for hash_tuple in new_hashes]

        return new_hashes

    except Exception as e:
        logging.error(f"compare_hashes error: {str(e)}")
        raise Exception(f"compare_hashes error: {str(e)}")


def return_rejected_hashes(hashes: List[str]) -> List[str]:
    """
    Returns hashes from the input list that exist in the database and have rejection_status = 'rejected'.

    :param engine: SQLAlchemy engine connection
    :param media_type: Type of media to determine the correct table
    :param hashes: List of hash strings to check
    :return: List of hashes that exist in the database and have rejection_status = 'rejected'
    """
    # assign engine
    engine = create_db_engine()

    try:
        # Query rejected hashes from database
        query = text(f"""
            WITH input_hashes (hash) AS (
                SELECT unnest(ARRAY{hashes}::text[]) as hash
            )
            SELECT input_hashes.hash
            FROM input_hashes
            JOIN media ON media.hash = input_hashes.hash
            WHERE media.rejection_status = 'rejected'
            AND media.deleted_at IS NULL;
        """)

        # Execute query and fetch results
        with engine.connect() as conn:
            rejected_hashes = conn.execute(query).fetchall()
            rejected_hashes = [hash_tuple[0] for hash_tuple in rejected_hashes]

        return rejected_hashes

    except Exception as e:
        logging.error(f"return_rejected_hashes error: {str(e)}")
        raise Exception(f"return_rejected_hashes error: {str(e)}")


def get_media_from_db(
    pipeline_status: str,
    with_timestamp: bool = False
) -> pl.DataFrame | None:
    """
    Retrieves data from movies or tv_shows table based on pipeline_status.

    :param pipeline_status: pipeline_status to filter by
    :param with_timestamp: whether to return timestamp fields
    :return: DataFrame containing matching rows
    """
    # assign engine
    engine = create_db_engine()

    query = text(f"""
        SELECT *
        FROM media
        WHERE pipeline_status = :pipeline_status
        AND error_status = FALSE
        AND deleted_at IS NULL
        ORDER BY hash
    """)

    params = {'pipeline_status': pipeline_status}

    with engine.connect() as conn:
        # Execute the query
        result = conn.execute(query, params)

        # Get column names from the result
        columns = result.keys()

        # Fetch all rows
        rows = result.fetchall()

        if not rows:
            return None

        # Convert to dict for polars
        data = [dict(zip(columns, row)) for row in rows]

    return pl.DataFrame(data)


def get_media_by_hash(
    hashes: list,
    with_timestamp: bool = False
) -> pl.DataFrame | None:
    """
    retrieves data from media by hash

    :param hashes: list of hashes to retrieve, if available
    :param with_timestamp: whether to return timestamp fields
    :return: DataFrame containing matching rows
    """
    # assign engine
    engine = create_db_engine()

    query = text(f"""
        SELECT *
        FROM media
        WHERE hash IN :hashes
        AND error_status = FALSE
        AND deleted_at IS NULL
    """)

    params = {'hashes': tuple(hashes)}

    with engine.connect() as conn:
        # Execute the query
        result = conn.execute(query, params)

        # Get column names from the result
        columns = result.keys()

        # Fetch all rows
        rows = result.fetchall()

        if not rows:
            return None

        # Convert to dict for polars
        data = [dict(zip(columns, row)) for row in rows]

    return pl.DataFrame(data)


def get_media_metadata(tmdb_ids: list) -> pl.DataFrame | None:
    """
    gets media metadata that already existing in the database training table

    :param tmdb_ids: list of strings in the form of The Movie Database ID's
    :return: polars DataFrame contains returned data
    """
    # assign engine
    engine = create_db_engine()

    # build query
    query = text(f"""
        SELECT *
        FROM training
        WHERE tmdb_id IN :tmdb_ids
    """)

    params = {'tmdb_ids': tuple(tmdb_ids)}

    with engine.connect() as conn:
        # Execute the query
        result = conn.execute(query, params)

        # Get column names from the result
        columns = result.keys()

        # Fetch all rows
        rows = result.fetchall()

        if not rows:
            return None

        # Convert to dict for polars
        data = [dict(zip(columns, row)) for row in rows]

    # return only relevant rows
    media_metadata = pl.DataFrame(data).drop(
        'label',
        'season',
        'episode',
        'created_at',
        'updated_at'
    )

    # Convert to polars DataFrame and wrap in MediaDataFrame
    return media_metadata


def get_training_metadata(imdb_ids: list) -> pl.DataFrame | None:
    """
    Gets training metadata by imdb_id for reel-driver predictions.

    :param imdb_ids: list of IMDB IDs
    :return: DataFrame containing metadata fields needed for predictions
    """
    # assign engine
    engine = create_db_engine()

    # build query - select only fields needed for reel-driver predictions
    query = text("""
        SELECT
            imdb_id,
            release_year,
            genre,
            spoken_languages,
            original_language,
            origin_country,
            production_countries,
            production_status,
            metascore,
            rt_score,
            imdb_rating,
            imdb_votes,
            tmdb_rating,
            tmdb_votes,
            budget,
            revenue,
            runtime,
            tagline,
            overview
        FROM training
        WHERE imdb_id IN :imdb_ids
    """)

    params = {'imdb_ids': tuple(imdb_ids)}

    with engine.connect() as conn:
        # Execute the query
        result = conn.execute(query, params)

        # Get column names from the result
        columns = result.keys()

        # Fetch all rows
        rows = result.fetchall()

        if not rows:
            return None

        # Convert to dict for polars
        data = [dict(zip(columns, row)) for row in rows]

    return pl.DataFrame(data)


def get_training_labels(imdb_ids: list) -> pl.DataFrame | None:
    """
    Gets training labels for items that should skip prediction.

    Currently disabled - all items go through reel-driver prediction
    regardless of existing labels.

    :param imdb_ids: list of strings in the form of IMDB ID's
    :return: None (all items should be predicted)
    """
    # All items should go through reel-driver prediction
    return None


# ------------------------------------------------------------------------------
# insert statements
# ------------------------------------------------------------------------------

def insert_items_to_db(media: pl.DataFrame):
    """
    Writes a DataFrame to the database using SQLAlchemy.

    :param media: DataFrame containing data to insert
    """
    # assign engine
    engine = create_db_engine()

    # Create SQLAlchemy table metadata
    metadata = MetaData(schema=pg_schema)
    sa_table = Table('media', metadata, autoload_with=engine)

    # Convert polars DataFrame to records
    records = media.to_dicts()

    logging.debug(f"attempting insert of {len(media)} records to table")

    # insert to database
    with engine.connect() as conn:
        transaction = conn.begin()
        try:
            result = conn.execute(sa_table.insert(), records)
            transaction.commit()
            inserted_rows = result.rowcount
            logging.debug(f"successfully inserted {inserted_rows} rows")
        except Exception as e:
            transaction.rollback()
            logging.error(f"error writing to database: {str(e)}")


# ------------------------------------------------------------------------------
# delete statements
# ------------------------------------------------------------------------------

def delete_items_from_db(hashes: list):
    """
    Deletes specified items from db.

    :param hashes: List of string value hashes
    """
    # assign engine
    engine = create_db_engine()

    # Create SQLAlchemy table metadata
    metadata = MetaData(schema=pg_schema)
    sa_table = Table('media', metadata, autoload_with=engine)

    logging.debug(f"attempting deletion of {len(hashes)} records")

    # insert to database
    with engine.connect() as conn:
        transaction = conn.begin()
        try:
            delete_stmt = sa_table.delete().where(sa_table.c.hash.in_(hashes))
            compiled_stmt = delete_stmt.compile(compile_kwargs={"literal_binds": True})
            logging.debug(compiled_stmt)
            result = conn.execute(delete_stmt)
            transaction.commit()
            logging.debug(f"successfully deleted {result.rowcount}")
        except Exception as e:
            transaction.rollback()
            logging.error(f"error writing to database: {str(e)}")


# ------------------------------------------------------------------------------
# update statements
# ------------------------------------------------------------------------------

def update_db_pipeline_status_by_hash(
    hashes: List[str],
    new_pipeline_status: str
):
    """
    Updates the pipeline_status for all rows matching the provided hash values.

    Parameters:
    hash_list (list): List of hash strings to update
    new_pipeline_status (str): New pipeline_status value to set

    Returns:
    int: Number of rows updated
    """
    # assign engine
    engine = create_db_engine()

    try:
        # Construct query with parameterized values for safety
        query = text(f"""
            UPDATE media 
            SET pipeline_status = :pipeline_status
            WHERE hash IN :hashes
        """)

        params = {
            'pipeline_status': new_pipeline_status,
            'hashes': tuple(hashes)
        }

        # Execute update
        with engine.connect() as conn:
            conn.execute(query, params)
            conn.commit()

    except Exception as e:
        raise Exception(f"Error updating pipeline_status: {str(e)}")


def update_rejection_status_by_hash(
    hashes: List[str],
    new_rejection_status: str
):
    """
    Updates the pipeline_status for all rows matching the provided hash values.

    :param hash_list: List of hash strings to update
    :param new_pipeline_status: New pipeline_status value to set
    :return: Number of rows updated
    """
    # assign engine
    engine = create_db_engine()

    try:
        # Construct query with parameterized values for safety
        query = text(f"""
            UPDATE media 
            SET rejection_status = :rejection_status
            WHERE hash IN :hashes
        """)

        params = {
            'rejection_status': new_rejection_status,
            'hashes': tuple(hashes)
        }

        # Execute update
        with engine.connect() as conn:
            conn.execute(query, params)
            conn.commit()

    except Exception as e:
        raise Exception(f"Error updating rejection_status: {str(e)}")


def media_db_update(media: pl.DataFrame) -> None:
    """
    Updates database records for media entries using SQLAlchemy's ORM approach.

    :param media: DataFrame containing media records to update
    """
    logging.debug(f"Starting database update for {len(media)} records")

    # warnings caused by loading elements to the PostgreSQL CHAR type
    warnings.filterwarnings(
        'ignore',
        message="Did not recognize type 'bpchar'",
        category=SAWarning
    )

    engine = create_db_engine()

    # Convert all polars nulls to None for SQLAlchemy compatibility
    records = []
    for row in media.iter_rows(named=True):
        clean_row = {k: (None if v is None or str(v) == "None" else v) for k, v in row.items()}
        records.append(clean_row)

    # Get table metadata
    metadata = MetaData()
    metadata.reflect(bind=engine, schema=pg_schema)
    table = metadata.tables[f"{pg_schema}.media"]

    # Create the upsert statement using SQLAlchemy
    stmt = insert(table).values(records)
    update_cols = {col.name: col for col in stmt.excluded if col.name != 'hash'}
    update_cols['updated_at'] = func.current_timestamp()

    # Create the upsert statement
    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=['hash'],
        set_=update_cols
    )

    logging.debug(f"Attempting upsert of {len(media)} records")

    try:
        with engine.begin() as conn:
            result = conn.execute(upsert_stmt)
            logging.debug(f"Successfully updated {result.rowcount} records")

    except Exception as e:
        logging.error(f"Error updating records: {str(e)}")
        raise

    finally:
        engine.dispose()


# ------------------------------------------------------------------------------
# training table operations
# ------------------------------------------------------------------------------

def training_db_upsert(training: pl.DataFrame) -> None:
    """
    Upserts training records to the atp.training table.

    On conflict (imdb_id), updates metadata fields only if human_labeled = false.
    Preserves label, human_labeled, anomalous, and reviewed flags on conflict.

    :param training: DataFrame containing training records to upsert
    """
    if training.height == 0:
        return

    logging.debug(f"Starting training upsert for {len(training)} records")

    # warnings caused by loading elements to the PostgreSQL CHAR type
    warnings.filterwarnings(
        'ignore',
        message="Did not recognize type 'bpchar'",
        category=SAWarning
    )

    engine = create_db_engine()

    # Convert all polars nulls to None for SQLAlchemy compatibility
    records = []
    for row in training.iter_rows(named=True):
        clean_row = {k: (None if v is None or str(v) == "None" else v) for k, v in row.items()}
        records.append(clean_row)

    # Get table metadata
    metadata = MetaData()
    metadata.reflect(bind=engine, schema=pg_schema)
    table = metadata.tables[f"{pg_schema}.training"]

    # Create the insert statement
    stmt = insert(table).values(records)

    # On conflict, update metadata columns only where human_labeled = false
    # Exclude: imdb_id (PK), label, human_labeled, anomalous, reviewed, created_at
    excluded_from_update = {'imdb_id', 'label', 'human_labeled', 'anomalous', 'reviewed', 'created_at'}
    update_cols = {
        col.name: col
        for col in stmt.excluded
        if col.name not in excluded_from_update
    }
    update_cols['updated_at'] = func.current_timestamp()

    # Create the upsert statement with condition
    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=['imdb_id'],
        set_=update_cols,
        where=table.c.human_labeled == False
    )

    logging.debug(f"Attempting training upsert of {len(training)} records")

    try:
        with engine.begin() as conn:
            result = conn.execute(upsert_stmt)
            logging.debug(f"Successfully upserted {result.rowcount} training records")

    except Exception as e:
        logging.error(f"Error upserting training records: {str(e)}")
        raise

    finally:
        engine.dispose()


def training_db_update_label(imdb_ids: List[str], label: str) -> None:
    """
    Updates the label for training records by imdb_id.

    Only updates records where human_labeled = false.

    :param imdb_ids: List of IMDB IDs to update
    :param label: New label value ('would_watch' or 'would_not_watch')
    """
    if not imdb_ids:
        return

    logging.debug(f"Updating label to '{label}' for {len(imdb_ids)} training records")

    engine = create_db_engine()

    try:
        query = text("""
            UPDATE training
            SET label = :label,
                updated_at = CURRENT_TIMESTAMP AT TIME ZONE 'UTC'
            WHERE imdb_id IN :imdb_ids
            AND human_labeled = FALSE
        """)

        params = {
            'label': label,
            'imdb_ids': tuple(imdb_ids)
        }

        with engine.connect() as conn:
            result = conn.execute(query, params)
            conn.commit()
            logging.debug(f"Successfully updated {result.rowcount} training labels")

    except Exception as e:
        logging.error(f"Error updating training labels: {str(e)}")
        raise

    finally:
        engine.dispose()


# ------------------------------------------------------------------------------
# end of sqlf.py
# ------------------------------------------------------------------------------