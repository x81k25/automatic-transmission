# ------------------------------------------------------------------------------
# create engine
# ------------------------------------------------------------------------------

from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

load_dotenv()

# create engine with params
engine = create_engine(
    url = URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv('PG_USERNAME'),
    password=os.getenv('PG_PASSWORD'),
    host=os.getenv('PG_ENDPOINT'),
    port=os.getenv('PG_PORT'),
    database=os.getenv('PG_DATABASE')
),
    pool_pre_ping=True,  # Enable connection health checks
    connect_args={'options': f'-csearch_path={os.getenv("PG_SCHEMA")}'},
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=1800,
    echo=False
)

# check connection
with engine.connect() as conn:
    conn.execute(text("SELECT 1")).scalar()

# ------------------------------------------------------------------------------
# select all
# ------------------------------------------------------------------------------

# create engine object

with engine.connect() as conn:
    query = text("SELECT * FROM test.movies")
    print(conn.execute(query).fetchall())

with engine.connect() as conn:
    query = text("SELECT * FROM test.movies")
    print(conn.execute(query).fetchall())


# ------------------------------------------------------------------------------
# compare hashes
# ------------------------------------------------------------------------------

hashes = ['hash1', 'hash2', 'hash3', 'hash4']

query = text(f"""
    WITH input_hashes (hash) AS (
        SELECT unnest(ARRAY{hashes}::text[]) as hash
    )
    SELECT input_hashes.hash
    FROM input_hashes
    LEFT JOIN test.movies ON test.movies.hash = input_hashes.hash
    WHERE test.movies.hash IS NULL;
""")

with engine.connect() as conn:
    result = conn.execute(query).fetchall()

# ------------------------------------------------------------------------------
# print schema structure
# ------------------------------------------------------------------------------

from sqlalchemy import create_engine, inspect, text, MetaData
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os

load_dotenv()

def inspect_database_schema(
    host=os.getenv('PG_ENDPOINT'),
    port=os.getenv('PG_PORT'),
    database=os.getenv('PG_DB'),
    user=os.getenv('PG_USER'),
    password=os.getenv('PG_PASSWORD'),
    schema=os.getenv("PG_SCHEMA"),
    dialect='postgresql'
):
    """
    Inspect and print detailed database schema information.

    Args:
        host (str): Database host address
        port (str): Database port
        database (str): Database name
        user (str): Database username
        password (str): Database password
        dialect (str): Database type ('postgresql', 'mysql', 'sqlite', etc.)
    """
    """
       Inspect and print detailed database schema information for a specific schema.

       Args:
           host (str): Database host address
           port (str): Database port
           database (str): Database name
           user (str): Database username
           password (str): Database password
           schema (str): Database schema name
       """
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"

    try:
        # Create engine and get inspector
        engine = create_engine(connection_string)
        inspector = inspect(engine)

        # Get all table names in the specified schema
        tables = inspector.get_table_names(schema=schema)
        print(f"\nFound {len(tables)} tables in schema '{schema}':")

        # Inspect each table
        for table_name in tables:
            print(f"\n{'-' * 50}")
            print(f"TABLE: {schema}.{table_name}")
            print(f"{'-' * 50}")

            # Get columns
            columns = inspector.get_columns(table_name, schema=schema)
            print("\nColumns:")
            for col in columns:
                nullable_str = "NULL" if col['nullable'] else "NOT NULL"
                print(f"  - {col['name']}: {col['type']} {nullable_str}")

            # Get primary keys
            pks = inspector.get_pk_constraint(table_name, schema=schema)
            if pks['constrained_columns']:
                print("\nPrimary Keys:")
                print(f"  - {', '.join(pks['constrained_columns'])}")

            # Get foreign keys
            fks = inspector.get_foreign_keys(table_name, schema=schema)
            if fks:
                print("\nForeign Keys:")
                for fk in fks:
                    print(f"  - {', '.join(fk['constrained_columns'])} -> "
                          f"{fk['referred_schema']}.{fk['referred_table']}.{', '.join(fk['referred_columns'])}")

            # Get indexes
            indexes = inspector.get_indexes(table_name, schema=schema)
            if indexes:
                print("\nIndexes:")
                for index in indexes:
                    unique_str = "UNIQUE " if index['unique'] else ""
                    print(
                        f"  - {unique_str}Index on ({', '.join(index['column_names'])})")

            # Get sample row count
            with engine.connect() as connection:
                result = connection.execute(
                    text(f"SELECT COUNT(*) FROM {schema}.{table_name}"))
                count = result.scalar()
                print(f"\nApproximate row count: {count}")

    except SQLAlchemyError as e:
        print(f"Error inspecting database: {str(e)}")

    finally:
        if 'engine' in locals():
            engine.dispose()

# Example usage:
inspect_database_schema()

# output

# ------------------------------------------------------------------------------
# schema structure
# ------------------------------------------------------------------------------

# --------------------------------------------------
# TABLE: test.movies
# --------------------------------------------------
# Columns:
#   - hash: VARCHAR(255) NOT NULL
#   - raw_title: TEXT NOT NULL
#   - movie_title: VARCHAR(255) NULL
#   - release_year: INTEGER NULL
#   - status: VARCHAR(50) NULL
#   - torrent_link: TEXT NULL
#   - rejection_reason: VARCHAR(255) NULL
#   - published_timestamp: TIMESTAMP NULL
#   - summary: TEXT NULL
#   - genre: ARRAY NULL
#   - language: ARRAY NULL
#   - metascore: INTEGER NULL
#   - rt_score: INTEGER NULL
#   - imdb_rating: NUMERIC(3, 1) NULL
#   - imdb_votes: INTEGER NULL
#   - imdb_id: VARCHAR(20) NULL
#   - resolution: VARCHAR(20) NULL
#   - video_codec: VARCHAR(50) NULL
#   - upload_type: VARCHAR(50) NULL
#   - audio_codec: VARCHAR(50) NULL
#   - file_name: TEXT NULL
#   - uploader: VARCHAR(100) NULL
#   - created_at: TIMESTAMP NULL
#   - updated_at: TIMESTAMP NULL
# Primary Keys:
#   - hash
# Indexes:
#   - Index on (imdb_id)
# Approximate row count: 0
#
# --------------------------------------------------
# TABLE: test.tv_shows
# --------------------------------------------------
# Columns:
#   - hash: VARCHAR(255) NOT NULL
#   - raw_title: TEXT NOT NULL
#   - tv_show_name: VARCHAR(255) NULL
#   - season: INTEGER NULL
#   - episode: INTEGER NULL
#   - status: VARCHAR(50) NULL
#   - magnet_link: TEXT NULL
#   - published_timestamp: TIMESTAMP NULL
#   - rejection_reason: TEXT NULL
#   - summary: TEXT NULL
#   - release_year: INTEGER NULL
#   - genre: ARRAY NULL
#   - language: ARRAY NULL
#   - metascore: INTEGER NULL
#   - imdb_rating: NUMERIC(3, 1) NULL
#   - imdb_votes: INTEGER NULL
#   - imdb_id: VARCHAR(20) NULL
#   - resolution: VARCHAR(20) NULL
#   - video_codec: VARCHAR(50) NULL
#   - upload_type: VARCHAR(50) NULL
#   - audio_codec: VARCHAR(50) NULL
#   - file_name: TEXT NULL
#   - created_at: TIMESTAMP NULL
#   - updated_at: TIMESTAMP NULL
# Primary Keys:
#   - hash
# Indexes:
#   - Index on (imdb_id)
#   - Index on (season, episode)
#   - Index on (tv_show_name)
# Approximate row count: 0

# ------------------------------------------------------------------------------
# test sql connection and packages
# ------------------------------------------------------------------------------

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os

load_dotenv()

def test_database_connection(
    host=os.getenv('PG_ENDPOINT'),
    port=os.getenv('PG_PORT'),
    database=os.getenv('PG_DB'),
    user=os.getenv('PG_USER'),
    password=os.getenv('PG_PASSWORD'),
    dialect='postgresql'
):
    connection_string = f"{dialect}://{user}:{password}@{host}:{port}/{database}"

    try:
        engine = create_engine(connection_string)
        with engine.connect() as connection:
            # Use text() to wrap the SQL query
            result = connection.execute(text("SELECT 1"))
            connection.commit()  # Commit the transaction

            # Fetch the result (optional)
            row = result.fetchone()
            return True, f"Successfully connected to the database! Test query returned: {row[0]}"

    except SQLAlchemyError as e:
        return False, f"Failed to connect to database. Error: {str(e)}"

    finally:
        if 'engine' in locals():
            engine.dispose()

# Example usage:
status, message = test_database_connection()
print(message)

# ------------------------------------------------------------------------------
# end of file tst_sql.py
# ------------------------------------------------------------------------------