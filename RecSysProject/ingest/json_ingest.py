#
#  ingest.json_ingest.py
#  Utility functions for loading large and/or complex JSON files. Specifically tailored to the Yelp dataset files.
#
#  Carson Rau - Fall 2023
#

import pandas as pd
import json
from multiprocessing import Pool, cpu_count
import logging
from sys import stdout
from ingest.json_ingest_ import *
from ingest.utils import get_path


# Load a json file into a pandas dataframe using a collection of worker threads in parallel.
#
# Parameters:
#   - path:         The relative path to the json data that should be loaded.
#   - encoding:     The json file encoding to use when parsing this file.
#                   This defaults to `utf-8` for the Yelp data.
#   - num_workers:  The maximum number of worker threads to use. When set to `None` (the default),
#                   this will be equivalent to the number of processor cores - 1.
# Returns:          The data, encoded in a pandas data frame.
def load_json_parallel(path, encoding='utf-8', num_workers=None, logger=logging.getLogger('load_json_parallel')):
    # The main loading logic
    if num_workers is None:
        num_workers = cpu_count() - 1
    logger.info(f'Parsing {path} with {num_workers} cpus.')
    pool = Pool(processes=num_workers)
    jobs = []

    for start, end in generate_chunks(path):
        jobs.append((path, start, end, encoding, logger))
    
    results = pool.map(process_chunk, jobs)
    pool.close()
    pool.join()
    rows = [row for result in results for row in result]
    return pd.DataFrame(rows)


# Load a json file into a pandas dataframe using a serial worker thread.
#
# Parameters:
#   - path:          The relative path to the json data that should be loaded.
#   - line_limit:    An optional artificial line limit to impose when reading large json
#                    files on reduced-memory systems. This value must be a positive integer, or None (the default).
#   - encoding:      The json file encoding to use when parsing this file.
#                    This defaults to `utf-8` for the Yelp data.
# Returns:           The data, encoded in a pandas data frame.
def load_json(path, line_limit=None, encoding='utf-8'):
    temp = []
    with open(path, encoding=encoding) as fl:
        for i, line in enumerate(fl):
            temp.append(json.loads(line))
            if line_limit is not None:
                if i + 1 > line_limit:
                    break
    return pd.DataFrame(temp)


# [[INTERNAL]]
# A test script to validate the same data is read by both the serial and parallel loading utilities.
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, stream=stdout)

    # Business Data
    business_path = get_path('business')
    logging.info(f'Loading data from {business_path}')
    data = load_json(business_path)
    print(f'The shape of the (serial) business data: {data.shape}')
    print(data.describe())
    data = load_json_parallel(business_path)
    print(f'The shape of the (parallel) business data: {data.shape}')
    print(data.describe())

    # Checkin Data
    checkin_path = get_path('checkin')
    logging.info(f'Loading data from {checkin_path}')
    data = load_json(checkin_path)
    print(f'The shape of the (serial) checkin data: {data.shape}')
    print(data.describe())
    data = load_json_parallel(checkin_path)
    print(f'The shape of the (parallel) checkin data: {data.shape}')
    print(data.describe())

    # Review Data
    review_path = get_path('review')
    logging.info(f'Loading data from {review_path}')
    data = load_json(review_path)
    print(f'The shape of the (serial) review data: {data.shape}')
    print(data.describe())
    data = load_json_parallel(review_path)
    print(f'The shape of the (parallel) review data: {data.shape}')
    print(data.describe())

    # Tip Data
    tip_path = get_path('tip')
    logging.info(f'Loading data from {tip_path}')
    data = load_json(tip_path)
    print(f'The shape of the (serial) tip data: {data.shape}')
    print(data.describe())
    data = load_json_parallel(tip_path)
    print(f'The shape of the (parallel) tip data: {data.shape}')
    print(data.describe())

    # User Data
    user_path = get_path('user')
    logging.info(f'Loading data from {user_path}')
    data = load_json(user_path)
    print(f'The shape of the (serial) user data: {data.shape}')
    print(data.describe())
    data = load_json_parallel(user_path)
    print(f'The shape of the (parallel) user data: {data.shape}')
    print(data.describe())