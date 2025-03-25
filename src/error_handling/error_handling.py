# standard imports
import logging

# custom and internal imports
import src.utils as utils

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

def clean_downloading_items(
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

# ------------------------------------------------------------------------------
# end of error_handling.py
# ------------------------------------------------------------------------------
