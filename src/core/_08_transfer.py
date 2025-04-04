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

def transfer_item(media_item: dict) -> dict:
	"""
	Transfer downloaded torrents to the appropriate directory
	:param media_item: media dict containing one row of media.df
	:return: updated media dict that contains error info if applicable
	"""

	try:
		if media_item['media_type'] == 'movie':
			utils.move_movie_local(
				dir_or_file_name=media_item['original_path'],
				download_dir=download_dir,
				movie_dir=movie_dir
			)
		elif media_item['media_type'] == 'tv_show':
			utils.move_tv_show_local(
				download_dir=download_dir,
				tv_show_dir=tv_show_dir,
				dir_or_file_name=media_item['original_path'],
				tv_show_name=media_item['media_title'],
				release_year=media_item['release_year'],
				season=media_item['season']
			)
		elif media_item['media_type'] == 'tv_season':
			utils.move_tv_season_local(
				download_dir=download_dir,
				tv_show_dir=tv_show_dir,
				dir_name=media_item['original_path'],
				tv_show_name=media_item['media_title'],
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

def transfer_media():
	"""
	full pipeline for cleaning up torrents that have completed transfer
	:return:
	"""
	# read in existing data based on ingest_type
	media = utils.get_media_from_db(pipeline_status='downloaded')

	# if not valid media items return
	if media is None:
		return

	# transfer media
	updated_rows = []
	for idx, row in enumerate(media.df.iter_rows(named=True)):
		updated_row = transfer_item(row)
		updated_rows.append(updated_row)

	media.update(pl.DataFrame(updated_rows))

	# update pipeline_status if no error occurred
	media.update(media.df.with_columns(
		pipeline_status = pl.when(pl.col('error_pipeline_status'))
			.then(pl.col('pipeline_status'))
			.otherwise(pl.lit('transferred'))
	))

	# output error if present
	for row in media.df.iter_rows(named=True):
		if row['error_status']:
			logging.error(f"{row['original_title']}: {row['error_condition']}")
		else:
			logging.info(f"transferred: {row['original_title']}")

	# update database
	utils.media_db_update(media=media)


# ------------------------------------------------------------------------------
# end of _08_transfer.py
# ------------------------------------------------------------------------------