#
#  parquet_ingest.py
#  Utility functions for loading compressed DataFrames to/from the Apache Parquet compressed data format.
#
#  Carson Rau - Fall 2023
#

import pandas as pd
import logging
from ingest.utils import get_path


def parquet_write(data_df, filepath, logger=logging.getLogger('parquet_write')):
    try:
        logger.info(f'Writing to file at {filepath}...')
        data_df.to_parquet(filepath, engine='pyarrow', compression='snappy')
    except Exception as e:
        logger.error(f'An error occurred while attempting to write {filepath}.', exc_info=True)
    logger.info(f'Done.')


def parquet_read(filepath, logger=logging.getLogger('parquet_read')):
    try:
        logger.info(f'Reading from file at {filepath}...')
        df = pd.read_parquet(filepath, engine='pyarrow')
        logger.info(f'Done.')
        return df
    except Exception as e:
        logger.error(f'An error occurred while attempting to read {filepath}.', exc_info=True)


if __name__ == '__main__':
    path = get_path('business_hours_data', directory='data_preprocess', is_yelp=False, is_json=False)
    logging.info(f'Attempting to read {path}')
    data = parquet_read(path)
    print(data.describe())
    print(data.shape)
