import os
from dotenv import load_dotenv
import src.utils as utils

# ------------------------------------------------------------------------------
# load environment variables
# ------------------------------------------------------------------------------

# Load environment variables from .env file
load_dotenv()

# set directories from .env
download_dir = os.getenv('DOWNLOAD_DIR')
tv_show_dir = os.getenv('TV_SHOW_DIR')
movie_dir = os.getenv('MOVIE_DIR')

# ------------------------------------------------------------------------------
# torrent clean-up functions
# ------------------------------------------------------------------------------

def transfer_item(media_item, media_type):
	"""
	Transfer downloaded torrents to the appropriate directory
	:param media_item: pd series of move or tv show to transfer
	:param media_type: type of transfer, either 'movie' or 'tv_show'
	:return:
	"""
	if media_type == 'movie':
		utils.move_movie(
			dir_or_file_name=media_item.file_name,
			download_dir=download_dir,
			movie_dir=movie_dir
		)
	elif media_type == 'tv_show':
		utils.move_tv_show(
			download_dir=download_dir,
			tv_show_dir=tv_show_dir,
			dir_or_file_name=media_item.file_name,
			tv_show_name=media_item.tv_show_name,
			release_year=media_item.release_year,
			season=media_item.season
		)
	else:
		raise ValueError('transfer_type must be either "movie" or "tv_show')

# ------------------------------------------------------------------------------
# torrent clean-up full pipelines
# ------------------------------------------------------------------------------

def transfer_media(media_type):
	"""
	Full pipeline for cleaning up torrents
	:param media_type: type of cleanup, either 'movie' or 'tv_show'
	:return:
	"""
	#media_type = 'movie'
	# read in existing data based on ingest_type
	engine = utils.create_db_engine()

	media = utils.get_media_from_db(
		engine=engine,
		media_type=media_type,
		status='downloaded'
	)

	# collects the hashes of torrents that have completed downloading
	hashes_transferred = []

	# transfer downloaded torrents
	if len(media) > 0:
		for index, row in media.iterrows():
			try:
				transfer_item(
					media_item=row,
					media_type=media_type
				)
				hashes_transferred.append(index)
				utils.log(
					f"transfer complete: {media.loc[index, 'raw_title']}")
			except Exception as e:
				utils.log(
					f"failed to transfer: {media.loc[index, 'raw_title']}")
				utils.log(f"transfer_item error: {e}")

	if len(hashes_transferred) > 0:
		# update status of relevant elements by hash
		utils.update_db_status_by_hash(
			engine=engine,
			media_type=media_type,
			hashes=hashes_transferred,
			new_status='complete'
		)

# ------------------------------------------------------------------------------
# end of _08_transfer.py
# ------------------------------------------------------------------------------