import numpy as np
import pickle
import pandas as pd
from src.utils import rpcf, logger, safe
import json

# ------------------------------------------------------------------------------
# read in static parameters
# ------------------------------------------------------------------------------

with open('./config/filter-parameters.json') as file:
    filters = json.load(file)

# ------------------------------------------------------------------------------
# initiation helper functions
# ------------------------------------------------------------------------------

def filter_item(item, filter_type):
    #filter_type = 'movie'
    # search separate criteria for move or tv_show
    if filter_type == 'movie':
        sieve = filters['movie']
        for key in sieve:
            if not sieve[key]["nullable"]:
                if pd.api.types.is_scalar(item[key]) and pd.isna(item[key]) or (not pd.api.types.is_scalar(item[key]) and pd.isna(item[key]).all()):
                    item['rejection_reason'] = f'{key} is null'
                    break
            if isinstance(item[key], str):
                if item[key] not in sieve[key]["allowed_values"]:
                    item['rejection_reason'] = f'{key} {item[key]} is not in allowed_values'
                    break
            elif pd.api.types.is_numeric_dtype(type(item[key])) or \
                 isinstance(item[key], (int, float, np.number)):
                if item[key] < sieve[key]["min"]:
                    item['rejection_reason'] = f'{key} {item[key]} is below min'
                    break
                elif item[key] > sieve[key]["max"]:
                    item['rejection_reason'] = f'{key} {item[key]} is above max'
                    break
            elif isinstance(item[key], (list, tuple, np.ndarray)) or \
               (isinstance(item[key], pd.Series) and len(item[key]) > 0):
                if not any([x in sieve[key]["allowed_values"] for x in item[key]]):
                    item['rejection_reason'] = f'{key} {item[key]} does not include {sieve[key]["allowed_values"]}'
                    break
    elif filter_type == 'tv_show':
        pass
    else:
        raise ValueError('filter_type must be either "movie" or "tv_show"')

    return item

def initiate_item(item, initiation_type):
    # Instantiate transmission client
    transmission_client = rpcf.get_transmission_client()

    # send link or magnet link to transmission
    if initiation_type == 'movie':
        transmission_client.add_torrent(item['torrent_link'])
    elif initiation_type == 'tv_show':
        transmission_client.add_torrent(item['magnet_link'])
    else:
        raise ValueError('initiation_type must be either "movie" or "tv_show')


# ------------------------------------------------------------------------------
# full initiation pipeline
# ------------------------------------------------------------------------------

def full_item_initiation(initiation_type):
    #initiation_type = 'tv_show'
    # read in existing data based on initiation_type
    if initiation_type == 'movie':
        master_df_dir = './data/movies.pkl'
    elif initiation_type == 'tv_show':
        master_df_dir = './data/tv_shows.pkl'
    else:
        raise ValueError('initiation_type must be either "movie" or "tv_show"')

    with open(master_df_dir, 'rb') as file:
        master_df = pickle.load(file)

    # select rows that have a status of queued
    hashes_to_filter = master_df[master_df['status'] == "queued"].index

    # filter through each row and update the status
    if len(hashes_to_filter) > 0:
        for index in hashes_to_filter:
            try:
                filtered_item = filter_item(
                    item=master_df.loc[index].copy(),
                    filter_type=initiation_type
                )
                master_df = safe.assign_row(master_df, index, filtered_item)
                if pd.notna(master_df.loc[index, 'rejection_reason']):
                    master_df = safe.update_status(master_df, index, 'rejected')
                    logger(f"rejected: {master_df.loc[index, 'raw_title']}: {master_df.loc[index, 'rejection_reason']}")
                else:
                    master_df = safe.update_status(master_df, index, 'filtered')
                    logger(f"filtered: {master_df.loc[index, 'raw_title']}")
            except Exception as e:
                logger(f"failed to filter: {master_df.loc[index, 'raw_title']}")
                logger(f"filter_item error: {e}")

    # select only that have a status of filtered
    hashes_to_initiate = master_df[master_df['status'] == "filtered"].index

    # iterate though each item that passed filtration, initiate download, and udpate status
    if len(hashes_to_initiate) > 0:
        for index in hashes_to_initiate:
            try:
                initiate_item(
                    item=master_df.loc[index].copy(),
                    initiation_type=initiation_type
                )
                master_df = safe.update_status(master_df, index, 'downloading')
                logger(f"downloading: {master_df.loc[index, 'raw_title']}")
            except Exception as e:
                logger(f"failed to download: {master_df.loc[index, 'raw_title']}")
                logger(f"initiate_item error: {e}")

    # Save the updated tv_shows DataFrame
    with open(master_df_dir, 'wb') as file:
        pickle.dump(master_df, file)

# ------------------------------------------------------------------------------
# testing
# ------------------------------------------------------------------------------

# initiate_tv_shows()