import os
from dotenv import load_dotenv
import pickle
from src.utils import rpcf, sshf, logger

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

def remove_item(item_to_remove):
    # Instantiate transmission client
    transmission_client = rpcf.get_transmission_client()

    torrent = transmission_client.get_torrent(item_to_remove.name)

    # remove completed downloads and update status
    if torrent.progress == 100.0:
        # get file name
        item_to_remove.file_name = torrent.name
        transmission_client.remove_torrent(item_to_remove.name)
        return item_to_remove
    else:
        return None


def transfer_item(item_to_transfer, transfer_type):
    """
    Transfer downloaded torrents to the appropriate directory
    :param item_to_transfer: pd series of move or tv show to transfer
    :param transfer_type: type of transfer, either 'movie' or 'tv_show'
    :return:
    """
    if transfer_type == 'movie':
        sshf.move_movie(
            dir_or_file_name=item_to_transfer.file_name,
            download_dir=download_dir,
            movie_dir=movie_dir
        )
    elif transfer_type == 'tv_show':
        sshf.move_tv_show(
            download_dir=download_dir,
            tv_show_dir=tv_show_dir,
            dir_or_file_name=item_to_transfer.file_name,
            tv_show_name=item_to_transfer.tv_show_name,
            release_year=item_to_transfer.release_year,
            season=item_to_transfer.season
        )
    else:
        raise ValueError('transfer_type must be either "movie" or "tv_show')


# ------------------------------------------------------------------------------
# torrent clean-up full pipelines
# ------------------------------------------------------------------------------

def full_item_cleanup(cleanup_type):
    """
    Full pipeline for cleaning up torrents
    :param cleanup_type: type of cleanup, either 'movie' or 'tv_show'
    :return:
    """
    # read in existing data based on cleanup_type
    #cleanup_type = 'tv_show'
    if cleanup_type == 'movie':
        master_df_dir = './data/movies.pkl'
    elif cleanup_type == 'tv_show':
        master_df_dir = './data/tv_shows.pkl'
    else:
        raise ValueError("invalid ingest type. Must be 'movie' or 'tv_show'")

    with open(master_df_dir, 'rb') as file:
        master_df = pickle.load(file)

    # collect hashes to check for completion
    hashes_to_remove = master_df[master_df['status'] == "downloading"].index

    # check for completion, remove if complete, and updates status
    if len(hashes_to_remove) > 0:
        for index in hashes_to_remove:
            try:
                removed_item = remove_item(
                    item_to_remove=master_df.loc[index].copy()
                )
                if removed_item is not None:
                    master_df.loc[index] = removed_item
                    master_df.loc[index, 'status'] = 'downloaded'
                    logger(f"downloaded: {master_df.loc[index, 'raw_title']}")
            except Exception as e:
                logger(f"failed to check status of: {master_df.loc[index, 'raw_title']}")
                logger(f"remove_item error: {e}")

    # collects the hashes of torrents that have completed downloading
    hashes_to_transfer = master_df[master_df['status'] == "downloaded"].index

    # transfer downloaded torrents
    if len(hashes_to_transfer) > 0:
        for index in hashes_to_transfer:
            try:
                transfer_item(
                    item_to_transfer=master_df.loc[index].copy(),
                    transfer_type=cleanup_type
                )
                master_df.loc[index, 'status'] = 'complete'
                logger(f"transfer complete: {master_df.loc[index, 'raw_title']}")
            except Exception as e:
                logger(f"failed to transfer: {master_df.loc[index, 'raw_title']}")
                logger(f"transfer_item error: {e}")

    # save the updated tv_shows DataFrame
    with open(master_df_dir, 'wb') as file:
        pickle.dump(master_df, file)


