# standard library imports
import logging
import os
from logging import exception

# third-party imports
from dotenv import load_dotenv
import polars as pl

# local/custom imports
import src.utils as utils

# ------------------------------------------------------------------------------
# load environment variables
# ------------------------------------------------------------------------------

# Load environment variables from .env file
load_dotenv()

# logger config
logger = logging.getLogger(__name__)

# set directories from .env
download_dir = os.getenv('DOWNLOAD_DIR')
tv_show_dir = os.getenv('TV_SHOW_DIR')
movie_dir = os.getenv('MOVIE_DIR')

# ------------------------------------------------------------------------------
# torrent clean-up functions
# ------------------------------------------------------------------------------

def transfer_item(
	media_item: dict,
	media_type: str
) -> dict:
	"""
	Transfer downloaded torrents to the appropriate directory
	:param media_item: media dict containing one row of media.df
	:param media_type: type of media, either 'movie', 'tv_show', or 'tv_season'
	:return: updated media dict that contains error info if applicable
	"""

	try:
		if media_type == 'movie':
			utils.move_movie_local(
				dir_or_file_name=media_item['file_name'],
				download_dir=download_dir,
				movie_dir=movie_dir
			)
		elif media_type == 'tv_show':
			utils.move_tv_show_local(
				download_dir=download_dir,
				tv_show_dir=tv_show_dir,
				dir_or_file_name=media_item['file_name'],
				tv_show_name=media_item['tv_show_name'],
				release_year=media_item['release_year'],
				season=media_item['season']
			)
		elif media_type == 'tv_season':
			utils.move_tv_season_local(
				download_dir=download_dir,
				tv_show_dir=tv_show_dir,
				dir_name=media_item['file_name'],
				tv_show_name=media_item['tv_show_name'],
				release_year=media_item['release_year'],
				season=media_item['season']
			)
	except Exception as e:
		media_item['error_status'] = True
		if media_item['error_condition'] is None:
			media_item['error_condition'] = f'failed to transfer media: {e}'
		else:
			media_item['error_condition'] = media_item['error_condition'] + \
				f'; failed to transfer media: {e}'

	return media_item

# ------------------------------------------------------------------------------
# torrent clean-up full pipelines
# ------------------------------------------------------------------------------

def transfer_media(media_type):
	"""
	full pipeline for cleaning up torrents that have completed transfer
	:param media_type: either "movies", "tv_show", or "tv_season"
	:return:
	"""
	#media_type = 'movie'

	# read in existing data based on ingest_type
	media = utils.get_media_from_db(
		media_type=media_type,
		status='downloaded'
	)

	# if not valid media items return
	if media is None:
		return

	# transfer media
	updated_rows = []
	for idx, row in enumerate(media.df.iter_rows(named=True)):
		# Modify your function to accept a dict instead of a Series
		updated_row = transfer_item(row, media_type)
		updated_rows.append(updated_row)

	media.update(pl.DataFrame(updated_rows))

	# update status if no error occurred
	media.df.with_columns(
		status = pl.when(pl.col('error_status'))
			.then(pl.col('status'))
			.otherwise(pl.lit('transferred'))
	)

	# output error if present
	for row in media.df.iter_rows(named=True):
		if row['error_status']:
			logging.error(f"{row['raw_title']}: {row['error_condition']}")
		else:
			logging.info(f"transferred: {row['raw_title']}")

	# update database
	utils.media_db_update(
		media_df=media,
		media_type=media_type
	)


# ------------------------------------------------------------------------------
# end of _08_transfer.py
# ------------------------------------------------------------------------------