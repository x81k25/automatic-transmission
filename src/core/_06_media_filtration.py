# standard library imports
import logging
import os
import requests

# third-party imports
from dotenv import load_dotenv

# local/custom imports
import src.utils as utils
import polars as pl
from src.data_models import MediaSchema, RejectionStatus, PipelineStatus, MediaType

# -----------------------------------------------------------------------------
# support functions that operate on one media item at a time
# -----------------------------------------------------------------------------

def get_prediction(media_item: dict) -> dict:
    """
    passes a single media_item to the reel-driver API and appends the
        probability to the media_item

    :param media_item: single media element as a dict
    :return: dict containing the original media item with probability attached
        or the error_condition if an error occurred

    :debug: media_item=next(media.df.iter_rows(named=True))
    """
    # reel_driver_env vars
    api_host = os.getenv('REEL_DRIVER_HOST')
    api_port = os.getenv('REEL_DRIVER_PORT')
    api_prefix = os.getenv('REEL_DRIVER_PREFIX')

    media_item['probability'] = None

    try:
        # construct API URL
        api_url = f"http://{api_host}:{api_port}/{api_prefix}/api/predict"

        # build payload
        payload = {
            'imdb_id': media_item.get('imdb_id'),
            'release_year': media_item.get('release_year'),
            'genre': media_item.get('genre'),
            'spoken_languages': media_item.get('spoken_languages'),
            'original_language': media_item.get('original_language'),
            'origin_country': media_item.get('origin_country'),
            'production_countries': media_item.get('production_countries'),
            'production_status': media_item.get('production_status'),
            'metascore': media_item.get('metascore'),
            'rt_score': media_item.get('rt_score'),
            'imdb_rating': media_item.get('imdb_rating'),
            'imdb_votes': media_item.get('imdb_votes'),
            'tmdb_rating': media_item.get('tmdb_rating'),
            'tmdb_votes': media_item.get('tmdb_votes'),
            'budget': media_item.get('budget'),
            'revenue': media_item.get('revenue'),
            'runtime': media_item.get('runtime'),
            'tagline': media_item.get('tagline'),
            'overview': media_item.get('overview')
        }

        # Call the API
        response = requests.post(api_url, json=payload)
        response.raise_for_status()

        media_item['probability'] = response.json()['probability']
    except Exception as e:
        media_item['error_condition'] = f"reel-driver API call failed - {e}"

    return media_item


# -----------------------------------------------------------------------------
# support functions that operate on multiple items as a DataFrame
# -----------------------------------------------------------------------------

def process_exempt_items(media: pl.DataFrame) -> pl.DataFrame:
    """
    processes items that do not need media filtering

    :param media: unprocessed DataFrame
    :return: updated DataFrame with the status updated to correspond
        with the label
    """
    if media.height == 0:
        return media

    media_exempt = media.clone()

    media_exempt = media_exempt.filter(
        pl.col('media_type').is_in([MediaType.TV_SHOW.value, MediaType.TV_SEASON.value])
    )

    return media_exempt


def reject_media_without_imdb_id(media: pl.DataFrame) -> pl.DataFrame:
    """
    set rejection_reason for items with no imdb_id

    :param media: DataFrame that contains elements potentially with
        and without imdb_id's
    :return: DataFrame of elements with no imdb_id
    """
    if media.height == 0:
        return media

    media_without_imdb_id = media.clone()

    media_without_imdb_id = media_without_imdb_id.filter(
        pl.col('imdb_id').is_null()
    ).with_columns(
        rejection_reason = pl.lit("no imdb_id for media filtration")
    )

    return media_without_imdb_id


def process_prelabeled_items(
    media: pl.DataFrame,
    media_labels: pl.DataFrame
) -> pl.DataFrame:
    """
    applies that retrieved media labels to the unprocessed DataFrame

    :param media: unprocessed DataFrame
    :param media_labels: DataFrame containing training labels and the
        corresponding imdb_id's
    :return: updated DataFrame with the status updated to correspond
        with the label
    """
    media_prelabeled = media.clone()

    # Add rejection_reason column if missing
    if 'rejection_reason' not in media_prelabeled.columns:
        media_prelabeled = media_prelabeled.with_columns(
            pl.lit(None).cast(pl.Utf8).alias('rejection_reason')
        )

    # join existing metadata with main DataFrame
    media_prelabeled = (
        media_prelabeled
        .filter(pl.col('imdb_id').is_in(media_labels['imdb_id'].to_list()))
        .join(
            media_labels,
            on='imdb_id',
            how='left'
        )
    )

    # update rejection_reason if needed accordingly
    media_prefiltered = media_prelabeled.with_columns(
        rejection_reason = pl.when(pl.col('label') == 'would_not_watch')
            .then(pl.lit('previously failed reel-driver'))
        .otherwise(pl.col('rejection_reason'))
    )

    return media_prefiltered


def get_predictions(media: pl.DataFrame) -> pl.DataFrame:
    """
    hits the reel-driver batch prediction, appends the probability to the
        input df, and then returns the df

    :param media: DataFrame containing all items to be predicted
    :return: DataFrame with the probability appended
    """
    # reel_driver_env vars
    api_host = os.getenv('REEL_DRIVER_HOST')
    api_port = os.getenv('REEL_DRIVER_PORT')
    api_prefix = os.getenv('REEL_DRIVER_PREFIX')

    media_with_predictions = media.clone()

    # Construct API URL
    api_url = f"http://{api_host}:{api_port}/{api_prefix}/api/predict_batch"

    # create payload with distinct imdb_id's only
    payload = {
        'items':
            media_with_predictions.unique(subset='imdb_id')
                .select
                    ([
                     'imdb_id',
                      'release_year',
                      'genre',
                      'spoken_languages',
                      'original_language',
                      'origin_country',
                      'production_countries',
                      'production_status',
                      'metascore',
                      'rt_score',
                      'imdb_rating',
                      'imdb_votes',
                      'tmdb_rating',
                      'tmdb_votes',
                      'budget',
                      'revenue',
                      'runtime',
                      'tagline',
                      'overview'
                ]).to_dicts()
    }

    # Call the API
    response = requests.post(api_url, json=payload)
    response.raise_for_status()

    # create new dataframe with results
    media_with_predictions = media_with_predictions.join(
        pl.DataFrame(response.json()['results']).select(
            'imdb_id',
            pl.col('probability').cast(pl.Float64)
        ),
        on = 'imdb_id',
        how = 'left'
    )

    return media_with_predictions


# -----------------------------------------------------------------------------
# support logic for updating status and displaying log output
# -----------------------------------------------------------------------------

def update_status(media: pl.DataFrame) -> pl.DataFrame:
    """
    properly updates all relevant status using the filtered DataFrame

    :param media: DataFrame with the probability attached or the
        error_condition if the probability could not be obtained
    :return: updated DataFrame with all errors properly tagged

    :debug: media = media_batch
    media_with_updated_status = pl.DataFrame(
             {
                    "hash": "errorstatus123456789012345678901234567890123",
                    "media_type": "movie",
                    "media_title": "Error Movie",
                    "release_year": 2020,
                    "pipeline_status": "metadata_collected",
                    "rejection_status": "accepted",
                    "rejection_reason": None,
                    "error_status": True
                }
    )


    """
    # load pipeline env vars
    acceptance_threshold = float(os.getenv('AT_REEL_DRIVER_THRESHOLD') or "0.35")

    media_with_updated_status = media.clone()

    # Add error_condition column if missing
    if 'error_condition' not in media_with_updated_status.columns:
        media_with_updated_status = media_with_updated_status.with_columns(
            pl.lit(None).cast(pl.Utf8).alias('error_condition')
        )

    # perform updates on items with predictions
    if 'probability' in media_with_updated_status.columns:
        # First ensure probability is Float64 for comparison
        media_with_updated_status = media_with_updated_status.with_columns(
            pl.col('probability').cast(pl.Float64)
        )

        media_with_updated_status = media_with_updated_status.with_columns(
            rejection_reason = (
                pl.when(pl.col('probability') < acceptance_threshold)
                    .then(pl.lit("probability ") + pl.col('probability').round(3).cast(pl.Utf8) + pl.lit(" below threshold ") + pl.lit(str(acceptance_threshold)))
                .otherwise(pl.col('rejection_reason'))
            )
        )

    # update rejection_status and pipeline_status
    media_with_updated_status = media_with_updated_status.with_columns(
        rejection_status = (
            pl.when(pl.col('rejection_status') == RejectionStatus.OVERRIDE.value)
                .then(pl.lit(RejectionStatus.OVERRIDE.value))
            .when(~pl.col('rejection_reason').is_null())
                .then(pl.lit(RejectionStatus.REJECTED.value))
            .otherwise(pl.lit(RejectionStatus.ACCEPTED.value))
        )).with_columns(
            pipeline_status = (
                pl.when(~pl.col('error_condition').is_null())
                    .then(pl.col('pipeline_status'))
                .when(pl.col('rejection_status') == RejectionStatus.OVERRIDE.value)
                    .then(pl.lit(PipelineStatus.MEDIA_ACCEPTED.value))
                .when(pl.col('rejection_status') == RejectionStatus.ACCEPTED.value)
                    .then(pl.lit(PipelineStatus.MEDIA_ACCEPTED.value))
                .when(pl.col('rejection_status') == RejectionStatus.REJECTED.value)
                    .then(pl.lit(PipelineStatus.REJECTED.value))
                .otherwise(pl.col('pipeline_status'))
            )
        )

    return media_with_updated_status


def log_status(media: pl.DataFrame) -> None:
    """
    logs acceptance/rejection of the media filtration process

    :param media: DataFrame contain process values to be printed
    :return: None
    """
    for row in media.iter_rows(named=True):
        if row['error_condition'] is not None:
            logging.error(f"{row['hash']} - {row['error_condition']}")
        elif row['rejection_status'] == RejectionStatus.REJECTED.value:
            logging.info(f"{row['rejection_status']} - {row['hash']} - {row['rejection_reason']}")
        elif 'probability' in row and row['probability'] is not None:
            logging.info(f"media-{row['rejection_status']} - {row['hash']} - with probability: {row['probability']:.3f}")
        else:
            logging.info(f"{row['pipeline_status']} - {row['hash']}")


# -----------------------------------------------------------------------------
# full media filtration pipeline
# -----------------------------------------------------------------------------

def filter_media():
    """
    full pipeline for filtering all media after metadata has been collected

    :debug: media = media[3]
    :debug: batch = 0
    """
    # pipeline env vars
    batch_size = int(os.getenv('AT_BATCH_SIZE') or "50")

    # read in existing data based on ingest_type
    media = utils.get_media_from_db(pipeline_status=PipelineStatus.METADATA_COLLECTED)

    if media is None:
        return

    # process items that do not need media filtering
    media_exempt = process_exempt_items(media)

    if media_exempt.height > 0:
        # update status, commit to db, and log
        media_exempt = update_status(media_exempt)
        utils.media_db_update(media=MediaSchema.validate(media_exempt))
        log_status(media_exempt)

        # remove from processing
        media = media.join(media_exempt.select('hash'), on='hash', how='anti')

    # if no further items to process return
    if media.height == 0:
        return

    # reject items with no valid imdb_id
    media_without_imdb_id = reject_media_without_imdb_id(media)

    if media_without_imdb_id.height > 0:
        # update status, commit to db, and log
        media_without_imdb_id = update_status(media_without_imdb_id)
        utils.media_db_update(media=MediaSchema.validate(media_without_imdb_id))
        log_status(media_without_imdb_id)

        # remove from processing
        media = media.join(media_without_imdb_id.select('hash'), on='hash', how='anti')

    # if no media with valid imdb_id, return
    if media.height == 0:
        return

    # get values for media which has previously been filtered
    media_labels = utils.get_training_labels(list(set(media['imdb_id'])))

    # if any labels have already been set
    if media_labels is not None:
        prelabeled_media = process_prelabeled_items(media, media_labels)

        # update status, commit to db, and log
        prelabeled_media = update_status(prelabeled_media)
        utils.media_db_update(media=MediaSchema.validate(prelabeled_media))
        log_status(prelabeled_media)

        # remove from list of items to be filtered
        media = media.join(prelabeled_media.select('hash'), on='hash', how='anti')

    # if no more items need filtration, return
    if media.height == 0:
        return

    # filter 1 item
    elif media.height == 1:
        prediction_result = get_prediction(media[0].to_dicts()[0])
        media_batch = pl.DataFrame([prediction_result])

        # Ensure probability is Float64 if it exists
        if 'probability' in media_batch.columns:
            media_batch = media_batch.with_columns(
                pl.col('probability').cast(pl.Float64)
            )

        # update status, commit to db, and log
        media_batch = update_status(media_batch)
        utils.media_db_update(MediaSchema.validate(media_batch))
        log_status(media_batch)

    # batch filter multiple items
    else:
        # break into batches to avoid entire queue failure due to 1 item
        number_of_batches = (media.height + (batch_size-1)) // batch_size  # Ceiling division by 50

        for batch in range(number_of_batches):
            logging.debug(f"starting media filtration batch {batch+1}/{number_of_batches}")

            # set batch indices
            batch_start_index = batch * batch_size
            batch_end_index = min((batch + 1) * batch_size, media.height)

            # create media batch
            media_batch = media[batch_start_index:batch_end_index].clone()

            try:
                # attempt to hit the prediction batch API
                media_batch = get_predictions(media_batch)

                # update statuses, commit to db, and log
                media_batch = update_status(media_batch)
                utils.media_db_update(media=MediaSchema.validate(media_batch))
                log_status(media_batch)

            except Exception as e:
                # log errors to individual elements
                logging.error(f"media filtration batch {batch+1}/{number_of_batches} failed - {e}")

                # attempt processing of items individually
                updated_rows = []

                for idx, row in enumerate(media_batch.iter_rows(named=True)):
                    updated_row = get_prediction(row)
                    updated_rows.append(updated_row)

                # Create DataFrame and ensure probability is Float64
                media_batch = pl.DataFrame(updated_rows)
                if 'probability' in media_batch.columns:
                    media_batch = media_batch.with_columns(
                        pl.col('probability').cast(pl.Float64)
                    )

                # update statuses, commit to db, and log
                media_batch = update_status(media_batch)
                utils.media_db_update(media=MediaSchema.validate(media_batch))
                log_status(media_batch)


# ------------------------------------------------------------------------------
# main guard
# ------------------------------------------------------------------------------

def main():
    utils.setup_logging()
    load_dotenv(override=True)
    filter_media()

if __name__ == "__main__":
    main()


# ------------------------------------------------------------------------------
# end of _05_filter.py
# ------------------------------------------------------------------------------