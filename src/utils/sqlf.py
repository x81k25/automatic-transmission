# standard library imports
import logging
import os
import sys
from typing import List, Optional

# third-party imports
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, Engine, Table, MetaData, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import URL
from sqlalchemy.sql import quoted_name

# internal imports
from src.data_models import MediaDataFrame

# ------------------------------------------------------------------------------
# load in environment variables
# ------------------------------------------------------------------------------
load_dotenv()

pg_username = os.getenv('PG_USERNAME')
pg_password = os.getenv('PG_PASSWORD')
pg_host = os.getenv('PG_ENDPOINT')
pg_port = os.getenv('PG_PORT')
pg_database = os.getenv('PG_DATABASE')
pg_schema = os.getenv('PG_SCHEMA')

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

    Parameters:
    username (str): Database username (default: PG_USER env var)
    password (str): Database password (default: PG_PASSWORD env var)
    host (str): Database host address (default: PG_ENDPOINT env var)
    port (str): Database port (default: PG_PORT env var)
    database (str): Database name (default: PG_DB env var)
    schema (str): Database schema (default: PG_SCHEMA env var or 'public')

    Returns:
    Engine: Configured database engine

    Raises:
    RuntimeError: If connection cannot be established with detailed error message
    ValueError: If required parameters are missing
    """
    required_params = {
        'username': username,
        'password': password,
        'host': host,
        'port': port,
        'database': database
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
# non SQL helper functions
# ------------------------------------------------------------------------------

def assign_table(media_type: str):
    """
    Assigns the table name based on the media type.

    Parameters:
    media_type (str): Type of media to assign table for

    Returns:
    str: Table name for the specified media type
    """
    schema_name = os.getenv('PG_SCHEMA')
    schema_name = quoted_name(os.getenv('PG_SCHEMA'), True)

    if media_type == 'movie':
        table_name = 'movies'
    elif media_type == 'tv_show':
        table_name = 'tv_shows'
    elif media_type == 'tv_season':
        table_name = 'tv_seasons'
    else:
        logging.error("media_type must be either 'movie' or 'tv_show'")
        raise ValueError('media_type must be either "movie" or "tv_show"')

    output = {
        'table_only': quoted_name(table_name, True),
        'schema_only': quoted_name(schema_name, True),
        'schema_and_table': quoted_name(f"{schema_name}.{table_name}", True)
    }

    return output

# ------------------------------------------------------------------------------
# select statements
# ------------------------------------------------------------------------------

def compare_hashes_to_db(
    media_type: str,
    hashes: List[str],
    status: str = None
):
    """
    Compares hashes in the input list against existing hashes in the database.

    Parameters:
    hash_list (list): List of hash strings
    engine: SQLAlchemy engine connection

    Returns:
    tuple: (new_hashes, existing_hashes)
        - new_hashes: List of hashes not in the database
        - existing_hashes: List of hashes that exist in the database
    """
    # assign engine if none provided
    engine = create_db_engine()

    # assign table and schema
    table = assign_table(media_type)['schema_and_table']

    # define status condition if exists
    status_condition = f"AND {table}.status = {status}" if status is not None else ""

    try:
        # Query existing hashes from database
        query = text(f"""
            WITH input_hashes (hash) AS (
                SELECT unnest(ARRAY{hashes}::text[]) as hash
            )
            SELECT input_hashes.hash
            FROM input_hashes
            LEFT JOIN {table} ON {table}.hash = input_hashes.hash
            WHERE {table}.hash IS NULL
            {status_condition};
        """)

        # Read existing hashes into a list
        with engine.connect() as conn:
            new_hashes = conn.execute(query).fetchall()
            new_hashes = [hash_tuple[0] for hash_tuple in new_hashes]

        return new_hashes

    except Exception as e:
        logging.error(f"compare_hashes error: {str(e)}")
        raise Exception(f"compare_hashes error: {str(e)}")


def return_rejected_hashes(
    media_type: str,
    hashes: List[str]
):
    """
    Returns hashes from the input list that exist in the database and have rejection_status = 'rejected'.

    Parameters:
    engine: SQLAlchemy engine connection
    media_type (str): Type of media to determine the correct table
    hashes (list): List of hash strings to check

    Returns:
    list: List of hashes that exist in the database and have rejection_status = 'rejected'
    """
    # assign engine
    engine = create_db_engine()

    # assign table and schema
    table = assign_table(media_type)['schema_and_table']

    try:
        # Query rejected hashes from database
        query = text(f"""
            WITH input_hashes (hash) AS (
                SELECT unnest(ARRAY{hashes}::text[]) as hash
            )
            SELECT input_hashes.hash
            FROM input_hashes
            JOIN {table} ON {table}.hash = input_hashes.hash
            WHERE {table}.rejection_status = 'rejected';
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
    media_type: str,
    status: str
) -> MediaDataFrame | None:
    """
    Retrieves data from movies or tv_shows table based on status.

    Args:
        media_type: str, either "movie" or "tv_show"
        status: str, status to filter by

    Returns:
        MediaDataFrame containing matching rows
    """
    # assign engine
    engine = create_db_engine()

    # assign table and schema
    table = assign_table(media_type)['schema_and_table']

    query = text(f"""
        SELECT *
        FROM {table}
        WHERE status = :status
        AND error_status = FALSE
    """)

    params = {
        'status': status
    }

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

    # Convert to polars DataFrame and wrap in MediaDataFrame
    return MediaDataFrame(data)


# ------------------------------------------------------------------------------
# insert statements
# ------------------------------------------------------------------------------

def insert_items_to_db(
    media_type: str,
    media: MediaDataFrame
):
    """
    Writes a MediaDataFrame to the database using SQLAlchemy.

    Parameters:
    media_type (str): Type of media ('movie', 'tv', etc.)
    media (MediaDataFrame): MediaDataFrame containing data to insert
    """
    #media_type = 'movie'
    #media = new_items

    # assign engine
    engine = create_db_engine()

    # assign table and schema
    table, schema = [assign_table(media_type)[key] for key in ['table_only', 'schema_only']]

    # Get the polars DataFrame
    pl_df = media.df

    # Create SQLAlchemy table metadata
    metadata = MetaData(schema=schema)
    sa_table = Table(table, metadata, autoload_with=engine)

    # Convert polars DataFrame to records
    records = pl_df.to_dicts()

    logging.debug(f"attempting insert of {len(media.df)} records to {media_type} table")

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

def delete_items_from_db(
    hashes: list,
    media_type: str
):
    """
    deletes specified items from db
    :param hashes: list of string value hashes
    :param media_type: either movies, tv_shows, or tv_seasons
    """
    # assign engine
    engine = create_db_engine()

    # assign table and schema
    table, schema = [assign_table(media_type)[key] for key in ['table_only', 'schema_only']]

    # Create SQLAlchemy table metadata
    metadata = MetaData(schema=schema)
    sa_table = Table(table, metadata, autoload_with=engine)

    logging.debug(f"attempting deletion of {len(hashes)} records from {media_type} table")

    # insert to database
    with engine.connect() as conn:
        transaction = conn.begin()
        try:
            delete_stmt = sa_table.delete().where(sa_table.c.hash.in_(hashes))
            logging.debug(delete_stmt)
            result = conn.execute(delete_stmt)
            transaction.commit()
            logging.debug(f"{result}")
        except Exception as e:
            transaction.rollback()
            logging.error(f"error writing to database: {str(e)}")


# ------------------------------------------------------------------------------
# update statements
# ------------------------------------------------------------------------------

def update_db_status_by_hash(
    media_type: str,
    hashes: List[str],
    new_status: str
):
    """
    Updates the status for all rows matching the provided hash values.

    Parameters:
    engine: SQLAlchemy engine connection
    hash_list (list): List of hash strings to update
    new_status (str): New status value to set

    Returns:
    int: Number of rows updated
    """
    # assign engine
    engine = create_db_engine()

    # assign table and schema
    table = assign_table(media_type)['schema_and_table']

    try:
        # Construct query with parameterized values for safety
        query = text(f"""
            UPDATE {table} 
            SET status = :status
            WHERE hash IN :hashes
        """)

        params = {
            'status': new_status,
            'hashes': tuple(hashes)
        }

        # Execute update
        with engine.connect() as conn:
            conn.execute(query, params)
            conn.commit()

    except Exception as e:
        raise Exception(f"Error updating status: {str(e)}")


def update_rejection_status_by_hash(
    media_type: str,
    hashes: List[str],
    new_status: str
):
    """
    Updates the status for all rows matching the provided hash values.

    Parameters:
    engine: SQLAlchemy engine connection
    hash_list (list): List of hash strings to update
    new_status (str): New status value to set

    Returns:
    int: Number of rows updated
    """
    # assign engine
    engine = create_db_engine()

    # assign table and schema
    table = assign_table(media_type)['schema_and_table']

    try:
        # Construct query with parameterized values for safety
        query = text(f"""
            UPDATE {table} 
            SET rejection_status = :rejection_status
            WHERE hash IN :hashes
        """)

        params = {
            'rejection_status': new_status,
            'hashes': tuple(hashes)
        }

        # Execute update
        with engine.connect() as conn:
            conn.execute(query, params)
            conn.commit()

    except Exception as e:
        raise Exception(f"Error updating status: {str(e)}")


def media_db_update(
    media: MediaDataFrame,
    media_type: str
) -> None:
    """
    Updates database records for media entries using SQLAlchemy's ORM approach.

    Parameters:
    media (MediaDataFrame): MediaDataFrame containing media records to update
    media_type (str): Type of media ('movie', 'tv_show', 'tv_season')
    """
    logging.debug(f"Starting database update for {len(media.df)} {media_type} records")

    table_info = assign_table(media_type)
    engine = create_db_engine()

    # Convert all polars nulls to None for SQLAlchemy compatibility
    # First convert to Python objects row by row
    records = []
    for row in media.df.iter_rows(named=True):
        # Replace polars.Null with None in each row
        clean_row = {k: (None if v is None or str(v) == "None" else v) for k, v in row.items()}
        records.append(clean_row)

    # Get table metadata
    metadata = MetaData()
    metadata.reflect(bind=engine, schema=table_info['schema_only'])
    table = metadata.tables[f"{table_info['schema_only']}.{table_info['table_only']}"]

    # Create the upsert statement using SQLAlchemy
    stmt = insert(table).values(records)
    update_cols = {col.name: col for col in stmt.excluded if col.name != 'hash'}
    update_cols['updated_at'] = func.current_timestamp()

    # Create the upsert statement
    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=['hash'],
        set_=update_cols
    )

    logging.debug(f"Attempting upsert of {len(media.df)} records to {media_type} table")

    try:
        with engine.begin() as conn:
            result = conn.execute(upsert_stmt)
            logging.debug(f"Successfully updated {result.rowcount} records in {media_type} table")

    except Exception as e:
        logging.error(f"Error updating {media_type} records: {str(e)}")
        raise

    finally:
        engine.dispose()


# ------------------------------------------------------------------------------
# end of sqlf.py
# ------------------------------------------------------------------------------