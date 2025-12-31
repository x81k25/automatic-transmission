# standard library imports
import logging

# third party imports
import polars as pl

# local/custom imports
from src.data_models import MediaSchema, RejectionStatus, PipelineStatus
import src.utils as utils

# ------------------------------------------------------------------------------
# supporting functions
# ------------------------------------------------------------------------------

def confirm_downloading_status(
    media: pl.DataFrame,
    current_media_items: dict | None
) -> pl.DataFrame:
    """
    determine if items tagged as pipeline_status = 'downloading' are in fact
        downloading

    :param media: DataFrame are all 'downloading' items
    :param current_media_items: dict of media current items in transmission
    :return:
    """
    if media.height == 0:
        return media

    media_not_downloading = media.clone()

    # if database items in downloading state, but no items with transmission
    #   label error condition, which will trigger re-ingest
    if current_media_items is None:
        media_not_downloading = media_not_downloading.with_columns(
            error_condition=pl.lit("not found within transmission")
        )
    # if only some items missing from transmission, label only the items
    #   missing
    else:
        current_hashes = list(current_media_items.keys())

        media_not_downloading = media_not_downloading.filter(
            ~pl.col('hash').is_in(current_hashes)
        ).with_columns(
            error_condition = pl.lit("not found within transmission")
        )

    return media_not_downloading


def extract_and_verify_filename(
    media: pl.DataFrame,
    downloaded_media_items: dict
) -> pl.DataFrame:
    """
    extracts, verifies, and inserts original_path to DataFrame

    :param media: DataFrame without original_path
    :param downloaded_media_items: metadata for items in transmission
    :return: dict with updated file_name or error information
    """
    media_with_paths = media.clone()

    # Add original_path column if missing
    if 'original_path' not in media_with_paths.columns:
        media_with_paths = media_with_paths.with_columns(
            pl.lit(None).cast(pl.Utf8).alias('original_path')
        )

    # Add error_condition column if missing
    if 'error_condition' not in media_with_paths.columns:
        media_with_paths = media_with_paths.with_columns(
            pl.lit(None).cast(pl.Utf8).alias('error_condition')
        )

    # get file paths
    original_file_paths = pl.DataFrame([
        {'hash': k, 'original_path': v['name']}
        for k, v in downloaded_media_items.items()
    ]).with_columns(
        original_path = pl.col("original_path").fill_null("None")
    ).with_columns(
        error_condition = pl.when(
            pl.col("original_path") == "None")
                .then(pl.lit("media_item.name returned None object"))
            .when(pl.col("original_path") == "")
                .then(pl.lit("media_item.name returned empty string"))
            .when(pl.col("original_path") == pl.col("hash"))
                .then(pl.lit("media_item.name contains item hash not filename"))
            .otherwise(pl.lit(None))
    ).with_columns(
        original_path = pl.when(
            pl.col('original_path') == "None")
                .then(pl.lit(None))
            .otherwise(pl.col('original_path'))
    )

    media_with_paths = media_with_paths.update(
        original_file_paths,
        on='hash'
    )

    return media_with_paths


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

    # Add rejection_reason column if missing
    if 'rejection_reason' not in media_with_updated_status.columns:
        media_with_updated_status = media_with_updated_status.with_columns(
            pl.lit(None).cast(pl.Utf8).alias('rejection_reason')
        )

    media_with_updated_status = media_with_updated_status.with_columns(
        pipeline_status = pl.when(pl.col('error_condition') == "not found within transmission")
            .then(pl.lit(PipelineStatus.INGESTED.value))
        .when(
            (pl.col('error_condition').is_null()) &
            (pl.col('rejection_status') != RejectionStatus.REJECTED.value)
        )
            .then(pl.lit(PipelineStatus.DOWNLOADED.value))
        .otherwise(pl.col('pipeline_status'))
    )

    # if re-ingesting remove flags
    media_with_updated_status = media_with_updated_status.with_columns(
        rejection_status = pl.when(pl.col('rejection_status') == RejectionStatus.OVERRIDE.value)
            .then(pl.lit(RejectionStatus.OVERRIDE.value))
            .otherwise(
                pl.when(pl.col('pipeline_status') == PipelineStatus.INGESTED.value)
                    .then(pl.lit(RejectionStatus.UNFILTERED.value))
                .otherwise(pl.col('rejection_status'))
            ),
        rejection_reason = pl.when(pl.col('pipeline_status') == PipelineStatus.INGESTED.value)
            .then(pl.lit(None))
            .otherwise(pl.col('rejection_reason')),
        error_condition = pl.when(pl.col('pipeline_status') == PipelineStatus.INGESTED.value)
            .then(pl.lit(None))
            .otherwise(pl.col('error_condition'))
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
        if row['pipeline_status'] == PipelineStatus.INGESTED.value:
            logging.error(f"re-ingesting - {row['hash']}")
        elif row['error_condition'] is not None:
            logging.error(f"{row['hash']} - {row['error_condition']}")
        elif row['rejection_status'] == RejectionStatus.REJECTED.value:
            logging.info(f"{row['rejection_status']} - {row['hash']} - {row['rejection_reason']}")
        elif row['pipeline_status'] != PipelineStatus.TRANSFERRED.value:
            logging.info(f"{row['pipeline_status']} - {row['hash']}")


# ------------------------------------------------------------------------------
# main check download function for all media items
# ------------------------------------------------------------------------------

def check_downloads():
    """
    check downloads for all downloading media elements, and extracts file_name
        if download is complete
    """
    # read in existing data based on ingest_type
    #   check for orphaned transferred items as well
    media_downloading = utils.get_media_from_db(pipeline_status='downloading')
    media_transferred = utils.get_media_from_db(pipeline_status='transferred')

    if media_downloading is None and media_transferred is None:
         media = None
    elif media_downloading is None and media_transferred is not None:
        media = media_transferred
    elif media_downloading is not None and media_transferred is None:
        media = media_downloading
    else:
        media = pl.concat([
            media_downloading,
            media_transferred
        ])

    # get current media item info
    current_media_items = utils.return_current_media_items()

    # if no media items and no transmission items, return
    if media is None and current_media_items is None:
        return

    # if not items in downloading or transferred state, but items exist in
    #   current_media_items, retrieve them by hash
    if media is None and current_media_items is not None:
        media = utils.get_media_by_hash(list(current_media_items.keys()))

    # process items for re-ingestion
    media_not_downloading = confirm_downloading_status(
        media,
        current_media_items
    )

    # re-ingest items not downloading if needed
    if media_not_downloading.height > 0:
        media_not_downloading = update_status(media_not_downloading)
        utils.media_db_update(media=MediaSchema.validate(media_not_downloading))
        log_status(media_not_downloading)

        # remove reingested items from processing
        media = media.join(media_not_downloading.select('hash'), on='hash', how='anti')

    # remove transferred items from processing
    media = media.filter(pl.col('pipeline_status') != PipelineStatus.TRANSFERRED.value)

    # if no current_media_items, return
    if current_media_items is None:
        return

    # filter by items with download complete
    downloaded_media_items = {k: v for k, v in current_media_items.items() if v['progress'] == 100.0}

    media = media.filter(pl.col('hash').is_in(downloaded_media_items))

    # if no items downloaded, return
    if media.height == 0:
        return

    # extract file names from completed downloads
    media = extract_and_verify_filename(media, downloaded_media_items)

    # update status, commit to db, and log
    media = update_status(media)
    utils.media_db_update(media=MediaSchema.validate(media))
    log_status(media)


# ------------------------------------------------------------------------------
# main guard
# ------------------------------------------------------------------------------

def main():
    utils.setup_logging()
    check_downloads()

if __name__ == "__main__":
    main()


# ------------------------------------------------------------------------------
# end of _08_download_check.py
# ------------------------------------------------------------------------------
