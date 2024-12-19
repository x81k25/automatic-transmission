from dotenv import load_dotenv
import os
import pandas as pd
import src.utils as utils

# ------------------------------------------------------------------------------
# load environment variables and
# ------------------------------------------------------------------------------

# Load environment variables from .env file
load_dotenv()

# set directories from .env
download_dir=os.getenv('download_dir')
tv_show_dir=os.getenv('tv_show_dir')
movie_dir=os.getenv('movie_dir')

# ------------------------------------------------------------------------------
# torrent clean-up functions
# ------------------------------------------------------------------------------

def remove_item(media_item):
    # Instantiate transmission client
    transmission_client = utils.get_transmission_client()

    torrent = transmission_client.get_torrent(media_item.name)

    # remove completed downloads and update status
    if torrent.progress == 100.0:
        # get file name
        media_item.file_name = torrent.name
        transmission_client.remove_torrent(media_item.name)
        return media_item
    else:
        return None

# ------------------------------------------------------------------------------
# torrent clean-up full pipelines
# ------------------------------------------------------------------------------

def cleanup_media(media_type):
    """
    Full pipeline for cleaning up torrents
    :param media_type: type of cleanup, either 'movie' or 'tv_show'
    :return:
    """
    #media_type = 'tv_show'
    # read in existing data based on ingest_type
    engine = utils.create_db_engine()

    media = utils.get_media_from_db(
        engine=engine,
        media_type=media_type,
        status='downloading'
    )

    # collect hashes to check for completion
    media_removed = pd.DataFrame()

    # check for completion, remove if complete, and updates status
    if len(media) > 0:
        media_removed = media.copy().iloc[0:0]

        for index, row in media.iterrows():
            try:
                removed_item = remove_item(
                    media_item=row
                )
                if removed_item is not None:
                    media_removed = pd.concat([media_removed, removed_item.to_frame().T])
                    utils.log(f"downloaded: {media_removed.loc[index, 'raw_title']}")
            except Exception as e:
                utils.log(f"failed to check status of: {media.loc[index, 'raw_title']}")
                utils.log(f"remove_item error: {e}")

    if len(media_removed) > 0:
        # update database with filename
        utils.update_db_media_table(
            engine=engine,
            media_type=media_type,
            media_old=media,
            media_new=media_removed
        )

        # update status of relevant elements by hash
        utils.update_db_status_by_hash(
            engine=engine,
            media_type=media_type,
            hashes=media_removed.index.tolist(),
            new_status='downloaded'
)

# ------------------------------------------------------------------------------
# end of _07_cleanup.py
# ------------------------------------------------------------------------------