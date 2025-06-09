# standard library imports
import logging
from pathlib import Path

# third-party imports
import yaml

# local/custom imports
import src.utils as utils
from src.data_models import *

# ------------------------------------------------------------------------------
# support functions
# ------------------------------------------------------------------------------

def filter_by_file_metadata(media_item: dict) -> dict:
    """
    filters media based off its file parameters, e.g. resolution, coded, etc.

    :param media_item:
    :return: dict containing the updated filtered data

    :debug: media_item = media.df.filter(pl.col('hash') == '054ce77d971194b9b6ff403df0543cfa31328718').to_dicts()[0]
    """
    # get filter params
    # for normal execution
    try:
        config_path = Path(__file__).parent.parent.parent / 'config' / 'filter-parameters.yaml'
    # for IDE/interactive execution
    except NameError:
        config_path = './config/filter-parameters.yaml'
    with open(config_path, 'r') as file:
        filters = yaml.safe_load(file)

    # search separate criteria for move or tv_show
    if media_item['media_type'] == 'movie':
        sieve = filters['movie']
        # iterate over each key in the filter-parameters.yaml file
        for key in sieve:
            # if key is defined as not nullable, then item will be rejected if not populated
            if not sieve[key]["nullable"]:
                if media_item[key] is None:
                    media_item['rejection_reason'] = f'{key} is null'
                    break
            if isinstance(media_item[key], str):
                if "allowed_values" in sieve[key] and media_item[key] not in sieve[key]["allowed_values"]:
                    media_item['rejection_reason'] = f'{key} {media_item[key]} is not in allowed_values'
                    break
            elif isinstance(media_item[key], (int, float)):
                if "min" in sieve[key] and media_item[key] < sieve[key]["min"]:
                    media_item['rejection_reason'] = f'{key} {media_item[key]} is below min'
                    break
                elif "max" in sieve[key] and media_item[key] > sieve[key]["max"]:
                    media_item['rejection_reason'] = f'{key} {media_item[key]} is above max'
                    break
            elif isinstance(media_item[key], list):
                if "allowed_values" in sieve[key] and not any([x in sieve[key]["allowed_values"] for x in media_item[key]]):
                    media_item['rejection_reason'] = f'{key} {media_item[key]} does not include {sieve[key]["allowed_values"]}'
                    break

    # there are currently no filters set for tv_show or tv_season
    elif media_item['media_type'] == 'tv_show':
        pass
    elif media_item['media_type'] == 'tv_season':
        pass

    return media_item


def update_status(media: MediaDataFrame) -> MediaDataFrame:
    """
    updates status flags based off of conditions

    :param media: MediaDataFrame with old status flags
    :return: updated MediaDataFrame with correct status flags
    """
    media_with_updated_status = media.df.clone()

    # update status accordingly
    media_with_updated_status = media_with_updated_status.with_columns(
            rejection_status = pl.when(pl.col('rejection_status') == RejectionStatus.OVERRIDE)
                .then(pl.lit(RejectionStatus.OVERRIDE))
            .when(pl.col('rejection_status') == RejectionStatus.REJECTED)
                .then(pl.lit(RejectionStatus.REJECTED))
            .otherwise(pl.lit(RejectionStatus.ACCEPTED))
        ).with_columns(
            pipeline_status = pl.when(pl.col('rejection_status') == RejectionStatus.OVERRIDE)
                .then(pl.lit(PipelineStatus.FILE_ACCEPTED))
            .when(pl.col('rejection_status') == RejectionStatus.ACCEPTED)
                .then(pl.lit(PipelineStatus.FILE_ACCEPTED))
            .otherwise(pl.lit(PipelineStatus.REJECTED))
        )

    return MediaDataFrame(media_with_updated_status)


def log_status(media: MediaDataFrame) -> None:
    """
    logs current stats of all media items

    :param media: MediaDataFrame contain process values to be printed
    :return: None
    """
    # log entries based on rejection status
    for idx, row in enumerate(media.df.iter_rows(named=True)):
        if row['rejection_status'] == RejectionStatus.OVERRIDE:
            logging.info(f"{row['rejection_status']} - {row['hash']}")
        elif row['pipeline_status'] == PipelineStatus.REJECTED:
            logging.info(f"{row['pipeline_status']} - {row['hash']} - {row['rejection_reason']}")
        else:
            logging.info(f"{row['pipeline_status']} - {row['hash']}")


# ------------------------------------------------------------------------------
# main function
# ------------------------------------------------------------------------------

def filter_files():
    """
    full pipeline for filtering all media items based off of the file metadata,
    	e.g. resolution, codec, media_type, etc.
    """
    # read in existing data based on ingest_type
    media = utils.get_media_from_db(pipeline_status='parsed')

    if media is None:
        return

    # filter based off of file parameters for all elements
    updated_rows = []
    for idx, row in enumerate(media.df.iter_rows(named=True)):
        try:
            updated_row = filter_by_file_metadata(media_item=row)
            updated_rows.append(updated_row)
        except Exception as e:
            error_message = f"file filtration error - {row['hash']} - {e}"
            logging.error(error_message)
            row['error_condition'] = error_message
            updated_rows.append(row)

    media = MediaDataFrame(updated_rows)

    # commit any items with errors to db, if any remove from working data set
    if media.df.filter(pl.col('error_status')).height > 0:
        utils.media_db_update(
            MediaDataFrame(
                media.df.filter(pl.col('error_status'))
            )
        )

    media.update(media.df.filter(~pl.col('error_status')))

    # if no items without errors, return
    if media.height == 0:
        return

    # update status
    media = update_status(media)

    # commit to db
    utils.media_db_update(media=media.to_schema())

    # log status
    log_status(media)


# ------------------------------------------------------------------------------
# main guard
# ------------------------------------------------------------------------------

def main():
    utils.setup_logging()
    filter_files()

if __name__ == "__main__":
    main()


# ------------------------------------------------------------------------------
# end of _05_filter.py
# ------------------------------------------------------------------------------