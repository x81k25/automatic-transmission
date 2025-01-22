import json
import src.utils as utils

# ------------------------------------------------------------------------------
# read in static parameters
# ------------------------------------------------------------------------------

with open('./config/filter-parameters.json') as file:
    filters = json.load(file)

# ------------------------------------------------------------------------------
# full initiation pipeline
# ------------------------------------------------------------------------------

def initiate_media_download(media_type):
    #media_type = 'tv_show'

    # read in existing data based on ingest_type
    media = utils.get_media_from_db(
        media_type=media_type,
        status='queued'
    )

    # select only that have a status of filtered
    hashes_initiated = []

    # iterate though each item that passed filtration, initiate download, and udpate status
    if len(media) > 0:
        for index in media.index.tolist():
            try:
                utils.add_media_item(media.loc[index, 'torrent_source'])
                hashes_initiated.append(index)
                utils.log(f"downloading: {media.loc[index, 'raw_title']}")
            except Exception as e:
                utils.log(f"failed to download: {media.loc[index, 'raw_title']}")
                utils.log(f"initiate_item error: {e}")

        if len(hashes_initiated) > 0:
            utils.update_db_status_by_hash(
                media_type=media_type,
                hashes=hashes_initiated,
                new_status='downloading'
            )

# ------------------------------------------------------------------------------
# end of _06_initiate.py
# ------------------------------------------------------------------------------