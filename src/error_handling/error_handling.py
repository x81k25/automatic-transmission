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
"1beb03f016f323331ef1d6986db62d7adc20fa44",
"9a406d05e19846af2c806c299c7bbc76a8cfa252",
"1dd556a88bd0eca58c7b33265ee7ac4f2cdc920d",
"5610bedad0f5b39b78582b8197c632f08a6036cf",
"932411ca300bc1e848f8ea26b8538ab25986a1cc",
"8a4aaebd2677c000f4af484a4a1ed154348f46b5",
"3a2015a608bddbbb5f386bbda8efffc62b014b1a",
"4f515ff1941e1258504621f85a793a736155f960",
"7d01923f04832809687548d214db1b8a6554784f",
"d0549bc8d774d8140610fb95a62adcf8cf8bf598",
"ba17c130e8954fab53cdf471ce697cc7fe4d4283",
"cfe1b90dbbdd64abc168e9bcae9dd8aebf564743",
"138ac106139787c779fcc3d9924a7f8ee91e3cd6",
"831060f7b729ecb68b071e144ac2294147578fc9",
"9c22e30a99e420533e31d77328d838397fb95427",
"08262ee9469659b098ea1e3748f51c7130737a6a",
"38c84e703759322ae805ba727bd4202c7221efed",
"b1a16cb34fcc65371bebe39ebb7b15d1b7b0c71a",
"a064d3414a353c69356b398c0b5c0c6192046155",
"f9a2c06947c928e71d6a96111711d096eabbddf0",
"fe850f33510ec5ef5ea65701568a107c276b6cfe",
"0aa4d5a114e3e381d0f278720ffe606f03250870",
"12c71832869d3957864f10c538b586caf05c892f",
"902246e9bc6076d26c7bf4838f983f90a5dd0056",
"b1c0ff2818e0b595c04a8d3326abea27dd3935f7",
"e11128783bbff5eb68df60327c4748a67ce7095e",
"08bea67a3205c1fa4955540c38d38a39d1a04deb",
"f345671caddb14af480be7521021775dfd9e99c4",
"19827c0a8196f98252bdee8055230924b2744b53",
"e5781de2ad387bad945661d936769ba21f0ede18",
"2a4f4be0b5dddf699ca5734a6e8977c55a3a2660",
"85f8f5862c1d5e04e19fee238c282f0d0d76cabe",
"ff4b1a9e01ec7866b8075d64950269c7279fbe6e",
"15332fd186a75b4bb6b414276a4c66718da1dee5",
"044313b2b2e5f4016a50a26ac735fbf124687f31",
"dd055ea29c345bba57487351d23079008e2d41cb",
"52fd638c2be4c0b755f4ae9e8c5a4b9254deb4ae",
"60fa4272aa48efe34b7d5c3ccff58e6d1f4747a9",
"b68027c1dbfc5c40e5984fcd3b442b0242e5b93f",
"34cc0f0ddb66f36fd580d0e138906dde507a6903"
]

reingest_items(hashes, 'stg')

mark_items_as_complete(hashes, 'prod')

reject_hung_downloads(hashes, 'prod')

# ------------------------------------------------------------------------------
# end of error_handling.py
# ------------------------------------------------------------------------------
