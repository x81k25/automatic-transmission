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
"08262ee9469659b098ea1e3748f51c7130737a6a",
"cfe1b90dbbdd64abc168e9bcae9dd8aebf564743",
"ba17c130e8954fab53cdf471ce697cc7fe4d4283",
"48cb49efad3ae54ff78d284ca152a54f43d53371",
"39300655fad4a221cb95e9fd43a8de9354bdaa5d",
"b5d122d045284d388888d4388f640adfdd06eefd",
"2863180ae4eb86c93a685206879bd16198021855",
"1036ac994e3828091bc2a264ad4599583c6533b8",
"96b47d6cf1b992c7c2e855b535eabda221061a61",
"85525001186e3b8640be21cca9fe3743084486bd",
"53c18d3ff923a7205257d10602d8349ef74862e1",
"03707ead102accbcc76d88b7ac9ea71d2daa930e",
"f1d8500b9540e03e175e733303046327fce7fc3b",
"a81e36a0dee60b6919c200d8998e6734b41d1fbd",
"38c17618200c98b0569265dd62c5ded7afb85b79",
"3b022075d06d565d14c7b02c41ee8b3b32e286e6",
"cecf1c35912807e27cc30a2d1db7d1c486c0109e",
"f6a57cb4dbf20c123c6b175adb4acfd097b2696d",
"80892ed1a8d9b7b1f6eee1e2197e36703369cbb8",
"9ab12d76ded0d8b15370c4f5049111787d9abeee",
"b3b51b13a4823bc34119c47c4a9014d679712232",
"b008aca7620938ecf588f084c16e910a5e86cb4b",
"f78af94128e4759bf754af76754ec97260ec97cf",
"9560d60b509e7e75ad72378a09ce20207db6906e",
"421fc48cb9b067cbfdac9a8816085bb5be7d9c10",
"f652d3e2d9d2ff8fa364cdc50b82d5a7634c51e4",
"2726fcc8834afd521a3ca0070ce363c0e39a0664",
"e8fb0bfc2b5a391c33f4bbd0edfb914c06b94302",
"9c88af11f9158512c0b5b1571cff021ce576d6ee",
"842c4a99be42e0a193587ebec5384afdc1e192d2",
"5e333d082255db31890c56f29119a38ddaa15e32",
"d61a1e5117745c6bf40eb6e17bd05a7adaaba334",
"4c3a8af9f125241efc38c1671e53c7640288073b",
"0664a9001d8ea990e2cc7c50b085e37c25f52a2a",
"af821908992b67ce4f11e02dc4625fab830ba551",
"46887b9e61e92035ea0db1b31a40d99affacfcd4",
"c9e10c046d468669a8dc3d04ce923d2e6d1ce5e3",
"68844f32da5b901cb8cf28eb3ee6ffed29a23767",
"24a65e34b7d3771ffd4988a951145dc11af90e8e",
"e2aa166e6d8c3db542aee60931a27ff74e48391f"
]

reingest_items(hashes, 'stg')

mark_items_as_complete(hashes, 'prod')

reject_hung_downloads(hashes, 'prod')

# ------------------------------------------------------------------------------
# end of error_handling.py
# ------------------------------------------------------------------------------
