# standard library imports
import logging
import os

# third party imports
from dotenv import load_dotenv
import polars as pl

# local/custom imports
from src.data_models import MediaSchema, RejectionStatus, PipelineStatus
import src.utils as utils

# ------------------------------------------------------------------------------
# utility functions
# ------------------------------------------------------------------------------

def initiate_media_item(media_item: dict) -> dict:
    """
    attempts to initiate media items, and if collects error information if
        initiation fails
    :param media_item: contains data of individual
    :return media_item: updated dict containing status and/or error data

    :debug:
    """
    # attempt to initiate each item
    try:
        utils.add_media_item(media_item['hash'])
    except Exception as e:
        media_item['error_condition'] = f"initiate_item error: {e}"

    return media_item


def update_status(media: pl.DataFrame) -> pl.DataFrame:
    """
    updates status flags based off of conditions

    :param media: DataFrame with old status flags
    :return: updated DataFrame with correct status flags
    """
    media_with_updated_status = media.clone()

    # Add error_condition column if missing
    if 'error_condition' not in media_with_updated_status.columns:
        media_with_updated_status = media_with_updated_status.with_columns(
            pl.lit(None).cast(pl.Utf8).alias('error_condition')
        )

    media_with_updated_status = media_with_updated_status.with_columns(
        pipeline_status = pl.when(
            (pl.col('rejection_status').is_in([RejectionStatus.ACCEPTED.value, RejectionStatus.OVERRIDE.value])) &
            (pl.col('error_condition').is_null())
        ).then(pl.lit(PipelineStatus.DOWNLOADING.value))
        .when(pl.col('rejection_status') == RejectionStatus.REJECTED.value)
            .then(pl.lit(PipelineStatus.REJECTED.value))
        .otherwise(pl.col('pipeline_status'))
    )

    return media_with_updated_status


def log_status(media: pl.DataFrame) -> None:
    """
    logs current stats of all media items

    :param media: DataFrame contain process values to be printed
    :return: None
    """
    # log entries based on rejection status
    for idx, row in enumerate(media.iter_rows(named=True)):
        if row['error_condition'] is not None:
            logging.error(f"{row['hash']} - {row['error_condition']}")
        elif row['rejection_status'] == RejectionStatus.REJECTED.value:
            logging.info(f"{row['rejection_status']} - {row['hash']} - {row['rejection_reason']}")
        else:
            logging.info(f"{row['pipeline_status']} - {row['hash']}")

# ------------------------------------------------------------------------------
# full initiation pipeline
# ------------------------------------------------------------------------------

def initiate_media_download():
    """
    attempts to initiate all media elements currently in the queued state

    :debug: batch=0
    """
    # pipeline env vars
    batch_size = int(os.getenv('AT_BATCH_SIZE') or "50")

    # read in existing data based
    media = utils.get_media_from_db(pipeline_status=PipelineStatus.MEDIA_ACCEPTED)

    # if no items to initiate, then return
    if media is None:
        return

     # batch up operations to avoid API rate limiting
    number_of_batches = (media.height + (batch_size-1)) // batch_size

    for batch in range(number_of_batches):
        logging.debug(f"starting initiation batch {batch+1}/{number_of_batches}")

        # set batch indices
        batch_start_index = batch * batch_size
        batch_end_index = min((batch + 1) * batch_size, media.height)

        # create media batch
        media_batch = media[batch_start_index:batch_end_index].clone()

        try:
            # initiate all queued downloads
            updated_rows = []
            for idx, row in enumerate(media_batch.iter_rows(named=True)):
                updated_row = initiate_media_item(row)
                updated_rows.append(updated_row)

            media_batch = pl.DataFrame(updated_rows)

            logging.debug(f"completed initiation batch {batch+1}/{number_of_batches}")

        except Exception as e:
            # log errors to individual elements
            media_batch = media_batch.with_columns(
                error_condition = pl.lit(f"batch error - {e}")
            )

            logging.error(f"initiation batch {batch+1}/{number_of_batches} failed - {e}")

        media_batch = update_status(media_batch)
        utils.media_db_update(media=MediaSchema.validate(media_batch))
        log_status(media_batch)


# ------------------------------------------------------------------------------
# main guard
# ------------------------------------------------------------------------------

def main():
    utils.setup_logging()
    load_dotenv(override=True)
    initiate_media_download()

if __name__ == "__main__":
    main()


# ------------------------------------------------------------------------------
# end of _07_initiate.py
# ------------------------------------------------------------------------------
