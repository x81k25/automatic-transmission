# standard library imports
import json
import logging

# third-party imports
import numpy as np
import pandas as pd

# local/custom imports
import src.utils as utils

# ------------------------------------------------------------------------------
# read in static parameters
# ------------------------------------------------------------------------------

with open('./config/filter-parameters.json') as file:
    filters = json.load(file)

# logger config
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# initiation helper functions
# ------------------------------------------------------------------------------

def filter_item(media_item, media_type):
    #filter_type = 'movie'
    # pass if override status was set
    if media_item['rejection_status'] == 'override':
        return media_item

    # search separate criteria for move or tv_show
    if media_type == 'movie':
        sieve = filters['movie']
        for key in sieve:
            if not sieve[key]["nullable"]:
                if pd.api.types.is_scalar(media_item[key]) and pd.isna(media_item[key]) or (not pd.api.types.is_scalar(media_item[key]) and pd.isna(media_item[key]).all()):
                    media_item['rejection_reason'] = f'{key} is null'
                    break
            if isinstance(media_item[key], str):
                if media_item[key] not in sieve[key]["allowed_values"]:
                    media_item['rejection_reason'] = f'{key} {media_item[key]} is not in allowed_values'
                    break
            elif pd.api.types.is_numeric_dtype(type(media_item[key])) or \
                 isinstance(media_item[key], (int, float, np.number)):
                if media_item[key] < sieve[key]["min"]:
                    media_item['rejection_reason'] = f'{key} {media_item[key]} is below min'
                    break
                elif media_item[key] > sieve[key]["max"]:
                    media_item['rejection_reason'] = f'{key} {media_item[key]} is above max'
                    break
            elif isinstance(media_item[key], (list, tuple, np.ndarray)) or \
               (isinstance(media_item[key], pd.Series) and len(media_item[key]) > 0):
                if not any([x in sieve[key]["allowed_values"] for x in media_item[key]]):
                    media_item['rejection_reason'] = f'{key} {media_item[key]} does not include {sieve[key]["allowed_values"]}'
                    break
    elif media_type == 'tv_show':
        pass
    elif media_type == 'tv_season':
        pass
    else:
        raise ValueError('filter_type must be either "movie", "tv_show", or "tv_season"')

    return media_item

# ------------------------------------------------------------------------------
# full initiation pipeline
# ------------------------------------------------------------------------------

def filter_media(media_type):
    #media_type = 'movie'

    # read in existing data based on ingest_type
    media = utils.get_media_from_db(
        media_type=media_type,
        status='metadata_collected'
    )

    # convert the index of the media to a column called hash
    media['hash'] = media.index

    # filter through each row and update the status
    media_filtered = pd.DataFrame()
    media_rejected = pd.DataFrame()

    if len(media) > 0:
        media_filtered = media.copy().iloc[0:0]
        media_rejected = media.copy().iloc[0:0]

        for index, row in media.iterrows():
            try:
                filtered_item = filter_item(
                    media_item=row,
                    media_type=media_type
                )
                if filtered_item['rejection_status'] == 'override':
                    media_filtered = pd.concat([media_filtered, filtered_item.to_frame().T])
                    logging.info(f"overridden: {media_filtered.loc[index, 'raw_title']}")
                elif filtered_item['rejection_reason'] is not None:
                    filtered_item['rejection_status'] = 'rejected'
                    media_rejected = pd.concat([media_rejected, filtered_item.to_frame().T])
                    logging.info(f"rejected: {media_rejected.loc[index, 'raw_title']}: {media_rejected.loc[index, 'rejection_reason']}")
                else:
                    filtered_item['rejection_status'] = 'accepted'
                    media_filtered = pd.concat([media_filtered, filtered_item.to_frame().T])
                    logging.info(f"accepted: {media_filtered.loc[index, 'raw_title']}")
            except Exception as e:
                logging.error(f"failed to filter: {media.loc[index, 'raw_title']}")
                logging.error(f"filter_item error: {e}")

    if len(media_rejected) > 0:
        # update database with for items that passed filtration
        utils.media_db_update(
            media_type=media_type,
            media_df=media_rejected
        )

        # update status
        utils.update_db_status_by_hash(
            media_type=media_type,
            hashes=media_rejected.index.tolist(),
            new_status='rejected'
        )

    if len(media_filtered) > 0:
        # update database for rejected items
        utils.media_db_update(
            media_type=media_type,
            media_df=media_filtered
        )

        # update status
        utils.update_db_status_by_hash(
            media_type=media_type,
            hashes=media_filtered.index.tolist(),
            new_status='queued'
        )

# ------------------------------------------------------------------------------
# end of _05_filter.py
# ------------------------------------------------------------------------------