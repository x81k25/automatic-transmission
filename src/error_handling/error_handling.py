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
"46e7803ff63b182b0c0b170943fbf649711d765e",
"b137df9121d5f6da6ea3b9ca36201a024257677b",
"9a406d05e19846af2c806c299c7bbc76a8cfa252",
"1dd556a88bd0eca58c7b33265ee7ac4f2cdc920d",
"932411ca300bc1e848f8ea26b8538ab25986a1cc",
"3a2015a608bddbbb5f386bbda8efffc62b014b1a",
"b823b5d8ec361c7e81c842011e5347ce74472400",
"a1007da8d56b09faef209627758b03697c37add2",
"0727c7ecca2a9a4a216f05c3a4ba450d4bb268f2",
"51b5b4a16b1bf935beff6e640437d3b69b2de409",
"1beb03f016f323331ef1d6986db62d7adc20fa44",
"5610bedad0f5b39b78582b8197c632f08a6036cf",
"2dd092421d8db2e6b93ece05d2425edc9a426770",
"e595890dc91dc43e2c8cc4bb1a22cad4613da730",
"b1c0ff2818e0b595c04a8d3326abea27dd3935f7",
"2c4946701380ea7c8372761c5a5611e6ec4c50c2",
"de1b6fd98ce65e16253426fae2db76b4e999c740",
"a88e4b3b5b9d0814729f8444211b072e2e60a4ac",
"e11128783bbff5eb68df60327c4748a67ce7095e",
"831060f7b729ecb68b071e144ac2294147578fc9",
"1b52865bdd7b07b9f7d0967b375f63d1b8f4e086",
"d2a51bba0779a7fff430b1b8e5e30c7e47344aac",
"0aa4d5a114e3e381d0f278720ffe606f03250870",
"75e9467b458e0f3118e180f74709b98a73808c48",
"8fffc4bb83fe24be9b504fefce425d4d30b499f4",
"efea7c19577eceee93928a694e05169f97be2c3f",
"124fbe29a33ca58039ce750d017081048f92bac9",
"c30057723482714100c3b9520c4657ce69e7bfd9",
"9ededaab55171991e43fbe5cc1f729173801caa3",
"04498b9f22cd7e612691d7dae37930cc6bac7b8a",
"1e5cde49a5c16ca8a4e8128996d0e2302117d8e9",
"444dd8d906af2e81a010446ef7e944af3afa996f",
"8c575966da4fe8fdd5d8781aefde2ea15f578e14",
"c5c07dcbd739a8e8b3b101ae4b9474ad2f145d1b"
]

reingest_items(hashes, 'stg')

mark_items_as_complete(hashes, 'prod')

reject_hung_downloads(hashes, 'prod')

# ------------------------------------------------------------------------------
# end of error_handling.py
# ------------------------------------------------------------------------------
