#
#  categories_ingest.py
#  Utility functions for stripping and parsing the comma-separated categories into a new binarized dataframe.
#
#  Carson Rau - Fall 2023
#

import pandas as pd
from ingest.json_ingest import load_json_parallel
import logging
from sys import stdout
from ingest.utils import get_path
from ingest.parquet_ingest import parquet_write


#  A function capable of splitting the comma separated strings contained in the business 'categories'
#  column into a new dataframe in which each unique category is a column, and each business entry is a row.
#  The presence, or lack, of a given category will be denoted by a boolean value within the dataframe.
#
#  Parameters:
#    - data:        The dataframe containing business information. This is expected to include the
#                   'business_id' and 'categories' columns.
#    - business_id  An optional business_id to narrow the category parsing. If this is None, all businesses in the
#                   dataframe are parsed.
#    - logger:      An optional logger to use for messages within this function. If none is provided, the
#                   'strip_categories' logger is used.
#  Returns:         The constructed categories dataframe.
def parse_categories(data, business_id=None, logger=logging.getLogger('strip_categories')):
    all_categories = set()
    data['categories'].dropna().str.split(',').apply(lambda cats: all_categories.update(cat.strip() for cat in cats))
    logger.info(f'All {len(data["categories"])} categories identified.')
    if business_id is not None:
        # Filter the data for the given business_id
        data = data[data['business_id'] == business_id]
        if data.empty:
            logger.warning(f"No data found for business_id: {business_id}")
            return pd.DataFrame()
        logger.info(f'Parsing data for business_id: {business_id}')
    else:
        logger.info(f'Parsing data for all businesses in dataframe.')
    logger.info(f'Building categories dataframe...')
    result = pd.DataFrame(columns=['business_id'] + list(all_categories))
    result['business_id'] = data['business_id']
    result = result.fillna(False)
    logger.info(f'Done.')
    logger.info(f'Populating...')

    def populate_categories(row):
        if pd.notna(row['categories']):
            categories = row['categories'].split(',')
            for category in categories:
                category = category.strip()
                result.at[row.name, category] = True

    data.apply(lambda row: populate_categories(row), axis=1)
    result.index = data.index
    logger.info(f'Categories dataframe complete.')
    return result


#  [[INTERNAL]]
#  A test script to verify category parsing for both 1 and many businesses.
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, stream=stdout)

    df = load_json_parallel(get_path('business'))
    logging.info(f'Loaded {get_path("business")}')
    categories_df = parse_categories(df)
    parquet_write(categories_df,
                  get_path('business_categories_data', directory='data_preprocess', is_yelp=False, is_json=False))
    print(f'The shape of the categories data: {categories_df.shape}')
    print(categories_df.describe())
