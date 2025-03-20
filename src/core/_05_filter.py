# standard library imports
import json
import logging

# third-party imports
import polars as pl

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

def filter_item(
    media_item: dict,
    media_type: str
) -> dict:
    """
    filters individual rows of media as dicts based off of the media_type
        and the parameters defined in filter-parameters.json
    :param media_item:
    :param media_type:
    :return: dict containing the updated filtered data
    """
    #filter_type = 'movie'

    # pass if override status was set
    if media_item['rejection_status'] == 'override':
        return media_item

    # search separate criteria for move or tv_show
    if media_type == 'movie':
        sieve = filters['movie']
        # iterate over each key in the filter-parameters.json file
        for key in sieve:
            # if key is defined as not nullable, then item will be rejected if not populated
            if not sieve[key]["nullable"]:
                if media_item[key] is None:
                    media_item['rejection_reason'] = f'{key} is null'
                    break
            if isinstance(media_item[key], str):
                if media_item[key] not in sieve[key]["allowed_values"]:
                    media_item['rejection_reason'] = f'{key} {media_item[key]} is not in allowed_values'
                    break
            elif isinstance(media_item[key], (int, float)):
                if media_item[key] < sieve[key]["min"]:
                    media_item['rejection_reason'] = f'{key} {media_item[key]} is below min'
                    break
                elif media_item[key] > sieve[key]["max"]:
                    media_item['rejection_reason'] = f'{key} {media_item[key]} is above max'
                    break
            elif isinstance(media_item[key], list):
                if not any([x in sieve[key]["allowed_values"] for x in media_item[key]]):
                    media_item['rejection_reason'] = f'{key} {media_item[key]} does not include {sieve[key]["allowed_values"]}'
                    break

    # there are currently no filters set for tv_show or tv_season
    elif media_type == 'tv_show':
        pass
    elif media_type == 'tv_season':
        pass

    return media_item

# ------------------------------------------------------------------------------
# full initiation pipeline
# ------------------------------------------------------------------------------

def filter_media(media_type: str):
    """
    full pipeline for filtering all media after metadata has been collected
    :param media_type: either "movie", "tv_show", or "tv_season"
    """
    #media_type = 'movie'

    # read in existing data based on ingest_type
    media = utils.get_media_from_db(
        media_type=media_type,
        status='metadata_collected'
    )

    if media is None:
        return

    # filter data
    updated_rows = []
    for idx, row in enumerate(media.df.iter_rows(named=True)):
        updated_row = filter_item(row, media_type)
        updated_rows.append(updated_row)

    media.update(pl.DataFrame(updated_rows))

    # update rejection status
    media.update(media.df.with_columns(
        rejection_status = pl.when(pl.col('rejection_reason').is_not_null())
            .then(pl.lit('rejected'))
            .otherwise(pl.col('rejection_status'))
    ))

    # log rejection entries
    for row in media.df.iter_rows(named=True):
        if row['error_status']:
            logging.error(f"{row['raw_title']}: {row['error_condition']}")
        elif row['rejection_status'] == 'rejected':
            logging.info(f"rejected: {row['raw_title']}: {row['rejection_reason']}")
        else:
            logging.info(f"queued: {row['raw_title']}")

    # update status
    media.update(media.df.with_columns(
        pl.when(pl.col('rejection_status') == 'rejected')
        .then(pl.lit('rejected'))
        .otherwise(pl.lit('queued'))
        .alias('status')
    ))

    utils.media_db_update(
        media_df=media,
        media_type=media_type
    )


# ------------------------------------------------------------------------------
# end of _05_filter.py
# ------------------------------------------------------------------------------