#
#  hours_ingest.py
#  Utility functions for parsing the hours dictionaries present in the Yelp business JSON.
#
#  Carson Rau - Fall 2023
#

from ingest.json_ingest import load_json_parallel, get_path
import pandas as pd
import logging
from sys import stdout
from ingest.hours_ingest_ import *
from ingest.utils import get_path
from ingest.parquet_ingest import parquet_write


def extract_hours_minutes(time_str):
    hh, mm = map(int, time_str.split(':'))
    return hh, mm


# An hours parsing function to convert the nested hours dictionary from within the business dataframe to a unique
# DataFrame containing the open and closing times as unique strings.
#
# Parameters:
#   - data:          The dataframe to parse.
#   - business_id:   An optional id to focus the attribute scraping to a single business, by string id.
#                    By default, this is None.
#   - logger:        An optional named logger to use within this function. If none is provided,
#                    parse_hours is used.
# Returns:           The fetched attributes dictionary.
def parse_hours(df, business_id=None, logger=logging.getLogger('parse_hours')):
    if business_id is not None:
        df = df[df['business_id'] == business_id]
        if df.empty:
            logger.warning(f"No entry found for business_id: {business_id}")
            return pd.DataFrame()  # Return an empty DataFrame
        logger.info(f'Parsing hours for business_id: {business_id}')
    else:
        logger.info(f'Parsing hours for entire dataframe.')

    result_df = df.apply(process_hours, axis=1)
    result_df['business_id'] = df['business_id'].values  # Add business_id to the results
    return result_df


# A preprocessing script. Parsing hours and saving the DataFrame to a json file.
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, stream=stdout)
    data = load_json_parallel(get_path('business'))
    logging.info(f'Loaded {get_path("business")}')

    hours_df = parse_hours(data)
    print(hours_df.describe())
    print(hours_df.info())
    parquet_write(hours_df,
                  get_path('business_hours_data', directory='data_preprocess', is_yelp=False, is_json=False))


    # Check for a single business
    # business = 'mWMc6_wTdE0EUBKIGXDVfA'
    # hours_df = parse_hours(data, business_id=business)
    # hours_df.to_json(get_path('hours', directory='data_analysis', is_yelp=False),
    #                 orient='records', lines=False, force_ascii=False, indent=4)
