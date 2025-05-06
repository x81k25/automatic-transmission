# standard imports
import logging

# third-party imports
import psycopg2
from psycopg2.extensions import connection

# custom and internal imports
import src.utils as utils
import yaml

# ------------------------------------------------------------------------------
# load environment variables and params
# ------------------------------------------------------------------------------

logging.basicConfig(
	level=logging.DEBUG,
	format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s',
	datefmt='%Y-%m-%d %H:%M:%S'
)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
logging.getLogger("paramiko").setLevel(logging.INFO)

# ------------------------------------------------------------------------------
# error_handling functions
# ------------------------------------------------------------------------------

def create_conn(env: str) -> psycopg2.extensions.connection:
	"""
	created connection object with given items from the connection yaml based
		off of the env provided

	:param env: env name as a string to connect to
	:return: psycopg2 connection based off of the env provided
	"""
	# Read the YAML configuration file
	try:
		with open('environments.yaml', 'r') as file:
			config = yaml.safe_load(file)
	except FileNotFoundError:
		raise FileNotFoundError("environments.yaml file not found")

	# Get the database configuration for the specified environment
	if env not in config['pgsql']:
		raise ValueError(f"Environment '{env}' not found in configuration")

	db_config = config['pgsql'][env]

	# Create the connection string
	conn_string = f"host={db_config['endpoint']} " \
				  f"port={db_config['port']} " \
				  f"dbname={db_config['database_name']} " \
				  f"user={db_config['username']} " \
				  f"password={db_config['password']}"

	# Connect to the database
	conn = psycopg2.connect(conn_string)

	# Set the schema as the default for lookups
	with conn.cursor() as cursor:
		cursor.execute(f"SET search_path TO {db_config['schema']}")
		conn.commit()

	return conn

# ------------------------------------------------------------------------------
# error_handling functions
# ------------------------------------------------------------------------------

def recycle_downloading_items(
	media_type: str,
	port: int = 9093,
	schema: str = "dev"
):
	"""
	- removes all items currently downloading from the database
	- upon rerun of pipeline, all elements this function removed should be
      collected and properly re-inserted into pipeline

	example execution:

	clean_downloading_items(
		media_type = "tv_show"
		,port = 9091
		,schema = "prod"
	)

	:param media_type: either "movies", "tv_shows", or "tv_seasons"
	:param port: the given port number for the cleaning;
	  - 9091 for prod
	  - 9092 for stg
	  - 9093 for dev (default)
	:param schema: matching schema name for port number
	"""
	#port = 9091

	# get all current downloading torrents
	current_items = utils.return_current_torrents(port=port)

	if current_items is None:
		logging.info("no items to clean")
		return

	current_hashes = list(current_items.keys())

	logging.debug(f"current hashes: {current_hashes}")

	# delete them from the database
	utils.delete_items_from_db(
		hashes=current_hashes,
		media_type=media_type,
		schema=schema
	)


def mark_items_as_complete(
	hashes: list,
	env: str
) -> int:
	"""
	- marks items as pipeline_status = "complete"
	- removes any error flags or conditions
	- set rejection status to override

	:param hashes: list of hashes as strings to be marked as complete
	:param env: either "prod" "stg" or "dev" used for mapping to appropriate
		ports and credentials
	:returns: int of the number of rows successfully updated
	"""
	# get database con
	conn = create_conn(env)

	# convert list to string
	hash_string = ",".join([f"'{h}'" for h in hashes])

	# construct udpate statement
	statement = f"""
	update atp.media 
	set pipeline_status = 'complete',
		error_status = False,
		error_condition = Null,
		rejection_status = 'override',
		rejection_reason = Null
	where hash in (
		{hash_string}
	)
	"""

	with conn.cursor() as cursor:
		cursor.execute(statement)
		rows_updated = cursor.rowcount
		conn.commit()

	conn.close()

	return rows_updated


def reingest_items(
	hashes: list,
	env: str
) -> int:
	"""
	takes elements at any stage in the ingestion pipeline and puts them back
		into the initial ingested state

	:param hashes: list of hashes as strings to be marked as complete
	:param env: either "prod" "stg" or "dev" used for mapping to appropriate
		ports and credentials
	:returns: int of the number of rows successfully updated
	"""
	# get database con
	conn = create_conn(env)

	# convert list to string
	hash_string = ",".join([f"'{h}'" for h in hashes])

	# construct udpate statement
	statement = f"""
	update atp.media
	set pipeline_status = 'ingested', 
		error_status = False,
		error_condition = Null,
		rejection_status = 'unfiltered',
		rejection_reason = Null
	where hash in (
		{hash_string}
	)
	"""

	with conn.cursor() as cursor:
		cursor.execute(statement)
		rows_updated = cursor.rowcount
		conn.commit()

	conn.close()

	return rows_updated


def reject_hung_downloads(
	hashes: list,
	env: str
) -> int:
	"""
	takes downloads that are just taking too long and updates status
		appropriately; in many cases these will be items that are redundant
		to other items that have already been successful; also remove the
		item from the daemon

	:param hashes: list of hashes as strings to be marked as complete
	:param env: either "prod" "stg" or "dev" used for mapping to appropriate
		ports and credentials
	:returns: int of the number of rows successfully updated
	"""
	# get database con
	conn = create_conn(env)

	# convert list to string
	hash_string = ",".join([f"'{h}'" for h in hashes])

	# construct udpate statement
	statement = f"""
	update atp.media
	set pipeline_status = 'rejected', 
		error_status = False,
		error_condition = Null,
		rejection_status = 'rejected',
		rejection_reason = 'download time limit exceeded'
	where hash in (
		{hash_string}
	)
	"""

	with conn.cursor() as cursor:
		cursor.execute(statement)
		rows_updated = cursor.rowcount
		conn.commit()

	conn.close()

	# needs to be properly connected to the correct environment
	for hash in hashes:
		utils.remove_media_item(hash)

	return rows_updated

# ------------------------------------------------------------------------------
# make error handling function calls
# ------------------------------------------------------------------------------

hashes = [
	"dceacb26f9f946bc72cc6301fa1bfafbeefdde1e"
]

reingest_items(hashes, 'prod')

mark_items_as_complete(hashes, 'prod')

reject_hung_downloads(hashes, 'prod')

# ------------------------------------------------------------------------------
# end of error_handling.py
# ------------------------------------------------------------------------------
