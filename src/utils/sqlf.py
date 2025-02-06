# standard library imports
import os
import logging
import sys
from typing import List, Optional

# third-party imports
from dotenv import load_dotenv
import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import quoted_name

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
    Creates and returns a SQLAlchemy engine for PostgreSQL connection with specified schema.
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
            #logging.error(f"Successfully connected to PostgreSQL database at {host}:{port}/{database}")
            return engine
    except:  # This will catch and suppress the connection error
        error_msg = (
            f"Unable to connect to database at {host}:{port}\n"
            f"Connection timed out. Please check:\n"
            f"- The database server is running\n"
            f"- The host and port are correct\n"
            f"- Any firewalls or network settings are blocking the connection"
        )
        logging.error(error_msg)
        sys.exit(1)  # This will exit the program silently

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
):
    """
    Retrieves data from movies or tv_shows table based on status.

    Args:
        engine: SQLAlchemy PostgreSQL engine
        query_type: str, either "movie" or "tv_show"
        status: str, status to filter by

    Returns:
        pandas.DataFrame containing matching rows
        :param media_type:
    """
    # assign engine
    engine = create_db_engine()

    # assign table and schema
    table = assign_table(media_type)['schema_and_table']

    query = text(f"""
        SELECT *
        FROM {table}
        WHERE status = :status
    """)

    params = {
        'status': status
    }

    with engine.connect() as conn:
        media = conn.execute(query, params).fetchall()
        media = pd.DataFrame(media)

    if "hash" in media.columns:
        media.set_index('hash', inplace=True)

    return media

# ------------------------------------------------------------------------------
# insert statements
# ------------------------------------------------------------------------------

def insert_items_to_db(
    media_type: str,
    media: DataFrame
):
    """
    Writes a DataFrame to the test.movies table.

    Parameters:
    movie_df (pd.DataFrame): DataFrame containing movie data
    engine: SQLAlchemy engine connection

    Returns:
    int: Number of rows inserted
    """
    #media_type = 'movie'
    #media = new_items

    # assign engine
    engine = create_db_engine()

    # assign table and schema
    table, schema = [assign_table(media_type)[key] for key in ['table_only', 'schema_only']]

    # Ensure all required columns are present
    required_columns = ['raw_title']
    missing_columns = [col for col in required_columns if
                       col not in media.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # Convert arrays to postgresql arrays
    if 'genre' in media.columns:
        media['genre'] = media['genre'].apply(
            lambda x: x if isinstance(x, list) else [])
    if 'language' in media.columns:
        media['language'] = media['language'].apply(
            lambda x: x if isinstance(x, list) else [])

    # Write to database
    try:
        inserted_rows = media.to_sql(
            table,
            engine,
            if_exists='append',
            index=True,
            index_label='hash',
            schema=schema,
            method='multi',
            chunksize=1000
        )
        #print(f"Successfully inserted {inserted_rows} rows into movies table")
    except Exception as e:
        raise Exception(f"Error writing to database: {str(e)}")


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


def update_db_media_table(
    media_type: str,
    media_old: DataFrame,
    media_new: DataFrame
):
    #media_old = media
    #media_new = media_parsed
    #media_type = 'movie'

    # assign engine
    engine = create_db_engine()

    # determine the columns from the media_old which are already populated
    populated_series = media_old.notna().any()
    populated_columns = populated_series[populated_series].index.tolist()

    # determine the columns from the media_new which are all na
    na_series = media_new.isna().all()
    na_columns = na_series[na_series].index.tolist()

    # crate a 3rd data frame that takes only the values that are populated in media_new and not in media_old
    media_update = media_new.drop(columns=na_columns + populated_columns)

    # assign fully qualified table name
    table = assign_table(media_type)['schema_and_table']

    #print(f"Starting update operation for {len(media_update)} rows in {table}")

    # Get the columns we want to update (excluding the index)
    update_columns = media_update.columns.tolist()

    # Initialize statistics
    stats = {
        "total_rows": len(media_update),
        "successful_updates": 0,
        "failed_updates": 0,
        "error_messages": []
    }

    try:
        # Build the dynamic UPDATE statement
        set_clause = ", ".join(
            [f"{col} = :new_{col}" for col in update_columns])
        update_stmt = text(f"""
            UPDATE {table}
            SET {set_clause}
            WHERE hash = :row_hash
        """)

        # Process rows in smaller batches to allow for partial success
        batch_size = 10
        for i in range(0, len(media_update), batch_size):
            batch = media_update.iloc[i:i + batch_size]

            with engine.begin() as conn:  # New transaction for each batch
                for idx, row in batch.iterrows():
                    try:
                        # Prepare parameters
                        params = {f"new_{col}": row[col] for col in
                                  update_columns}
                        params["row_hash"] = idx

                        # Execute the update
                        result = conn.execute(update_stmt, params)

                        if result.rowcount == 1:
                            stats["successful_updates"] += 1
                        else:
                            stats["failed_updates"] += 1
                            stats["error_messages"].append(
                                f"No row updated for hash {idx}. Row might not exist."
                            )

                    except SQLAlchemyError as e:
                        stats["failed_updates"] += 1
                        stats["error_messages"].append(
                            f"Error updating row with hash {idx}: {str(e)}"
                        )

    except Exception as e:
        #print(f"Fatal error during update operation: {str(e)}")
        stats["error_messages"].append(f"Fatal error: {str(e)}")

    #print(f"Update operation completed. "
    #      f"Successful updates: {stats['successful_updates']}, "
    #      f"Failed updates: {stats['failed_updates']}")

    return stats

# ------------------------------------------------------------------------------
# end of sqlf.py
# ------------------------------------------------------------------------------