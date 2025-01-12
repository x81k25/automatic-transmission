import pandas as pd
import re
import src.utils as utils

# ------------------------------------------------------------------------------
# collector helper functions
# ------------------------------------------------------------------------------

def classify_media(raw_title):
	"""
	Classify media as either movie or tv_show based on the title

	:param raw_title: raw title string of media item
	:param media_type: type of media to classify
	:return: string indicating media type
	"""

	# Core patterns
	tv_pattern = r'[Ss]\d{2}[Ee]\d{2}'
	movie_pattern = r'\(\d{4}\)'

	if re.search(tv_pattern, raw_title):
		return "tv_show"
	elif re.search(movie_pattern, raw_title):
		return "movie"

	return "unknown"


def extract_tv_show_name(raw_title):
	"""
	Extract the tv show name from the raw_title
	:param raw_title: raw_title string of media item
	:return: string of tv show name
	"""

	# Remove quality info and encoding info
	cleaned = re.sub(r'\d{3,4}p.*$', '', raw_title)

	# Get everything before the SxxExx pattern
	show_name = re.split(r'[Ss]\d{2}[Ee]\d{2}', cleaned)[0]

	# Clean up trailing/leading spaces and quotes
	show_name = show_name.strip(' "')

	return show_name


def collected_torrents_to_dataframe(media_items, media_type):
	formatted_df = pd.DataFrame()

	if media_type == 'movie':
		formatted_df =  pd.DataFrame(
		   data={
			   'hash': media_items['hash'],
			   'raw_title': media_items['name'],
			   'torrent_link': media_items['torrent_link']
		   }
		)
	elif media_type == 'tv_show':
		formatted_df = pd.DataFrame(
			data={
				'hash': media_items['hash'],
				'raw_title': media_items['name'],
				'tv_show_name': media_items['name'].apply(extract_tv_show_name),
				'magnet_link': media_items['magnet_link']
			}
		)

	# set hash as index
	formatted_df.set_index('hash', inplace=True)

	return formatted_df

# ------------------------------------------------------------------------------
# collect main function
# ------------------------------------------------------------------------------

def collect_media(media_type):
	"""
	collect ad hoc items added to transmission not from rss feeds and insert
	into automatic-transmission pipeline

	:param media_type: type of media to collect, either 'movie' or 'tv_show'
	"""
	#media_type = "tv_show"
	# get torrents currently in transmission
	# if no torrents in transmission end function
	current_media_items = utils.return_current_torrents()

	if current_media_items is None:
		return

	# determine if movie or tv_show
	current_media_items['media_type'] = current_media_items['name'].apply(classify_media)

	# split into tv_show and movie_dataframes
	collected_media_items = current_media_items[current_media_items['media_type'] == media_type]

	# instantiate db engine
	pg_engine = utils.sqlf.create_db_engine()

	# end function if torrent queue is empty
	if len(collected_media_items) == 0:
		return

	# determine which items are new
	new_hashes = utils.compare_hashes_to_db(
		media_type=media_type,
		hashes=list(collected_media_items['hash']),
		engine=pg_engine
	)

	# if no new items terminate function
	if len(new_hashes) == 0:
		return

	# filter out new items
	new_items = collected_media_items[collected_media_items['hash'].isin(new_hashes)]

	# format for db ingestion
	formatted_items = collected_torrents_to_dataframe(
		media_items=new_items,
		media_type=media_type
	)

	# insert to db
	utils.insert_items_to_db(
		media_type=media_type,
		media=formatted_items,
		engine=pg_engine
	)

	# update status
	utils.update_db_status_by_hash(
		engine=pg_engine,
		media_type=media_type,
		hashes=list(formatted_items.index),
		new_status='ingested'
	)

	# print log
	for index in formatted_items.index:
		utils.log(f"collected: {formatted_items.loc[index, 'raw_title']}")

# ------------------------------------------------------------------------------
# end of _02_collect.py
# ------------------------------------------------------------------------------