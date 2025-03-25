# standard imports
import logging

# custom and internal imports
import src.utils as utils
from src.utils import return_current_torrents

# ------------------------------------------------------------------------------
# load environment variables and
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# error_handling functions
# ------------------------------------------------------------------------------

def clean_downloading_items(
	media_type: str,
	port: int = 9093
):
	"""
	- removes all items currently downloading from the database
	- upon rerun of pipeline, all elements this function removed should be
      collected and properly re-inserted into pipeline
	:param media_type: either "movies", "tv_shows", or "tv_seasons"
	:param port: the given port number for the cleaning;
	  - 9091 for prod
	  - 9092 for stg
	  - 9093 for dev (default)
	"""
	# get all current downloading torrents
	current_items = utils.return_current_torrents()

	current_hashes = list(current_items.keys())

	logging.debug(f"current hashes: {current_hashes}")

	# delete them from the database
	utils.delete_items_from_db(current_hashes, media_type)

#clean_downloading_items(media_type="movie",port=9093)

# ------------------------------------------------------------------------------
# end of error_handling.py
# ------------------------------------------------------------------------------
