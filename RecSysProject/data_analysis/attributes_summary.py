#
#  attributes_summary.py
#  Utility functions for stripping and parsing the nested attributes dictionary from within a business' entry in the
#  business JSON file.
#
#  Carson Rau - Fall 2023
#

import ast
import json
from ingest.json_ingest import load_json_parallel
import logging
import re
from sys import stdout
import pandas as pd
from ingest.utils import get_path

# [[INTERNAL]]
# After examining business_attributes.json, these attributes use an enum-like string representation in the JSON file.
string_attributes = [
    'WiFi', 'Alcohol', 'RestaurantsAttire', 'Smoking', 'BYOBCorkage', 'AgesAllowed', 'NoiseLevel'
]


# [[INTERNAL]]
# A parsing function for attributes marked as `string_attributes`.
def parse_string_attributes(attributes, key, value, include_false):
    value = re.sub(r"^u['\"]", r'', value)
    value = value.replace("'", "").replace('"', "")
    if not include_false:
        if not (value.lower() == 'none'):
            attributes[key].add(value)
    else:
        attributes[key].add(value)
    return


# [[INTERNAL]]
# After examining business_attributes.json, these attributes use a numeric representation in the JSON file.
numeric_attributes = ['RestaurantsPriceRange2']


# [[INTERNAL]]
# A parsing function for attributes marked as `numeric_attributes`.
def parse_numeric_attributes(attributes, key, value, include_false):
    try:
        value = int(value)
    except ValueError:
        value = None
    if not include_false:
        if not (value is None):
            attributes[key].add(value)
    else:
        attributes[key].add(value)


# [[INTERNAL]]
# After examining business_attributes.json, these attributes use a boolean representation in the JSON file.
boolean_attributes = [
    'ByAppointmentOnly', 'BusinessAcceptsCreditCards', 'BikeParking',
    'CoatCheck', 'RestaurantsTakeOut', 'RestaurantsDelivery', 'Caters',
    'WheelchairAccessible', 'HappyHour', 'OutdoorSeating', 'HasTV', 'RestaurantsReservations',
    'DogsAllowed', 'GoodForKids', 'RestaurantsTableService', 'RestaurantsGoodForGroups', 'DriveThru',
    'BusinessAcceptsBitcoin', 'GoodForDancing', 'AcceptsInsurance', 'BYOB', 'Corkage', 'Open24Hours',
    'RestaurantsCounterService'
]


# [[INTERNAL]]
# A parsing function for attributes marked as `boolean_attributes`.
def parse_boolean_attributes(attributes, key, value, include_false):
    if value == 'False':
        value = False
    elif value == 'True':
        value = True
    else:
        value = None
    if not include_false:
        if value:
            attributes[key].add(value)
    else:
        attributes[key].add(value)


# [[INTERNAL]]
# After examining business_attributes.json, these attributes use a nested dictionary representation in the JSON file.
# For these attributes, the nested dictionary contains string,boolean pairs.
boolean_dict_attributes = [
    'BusinessParking', 'Ambience', 'GoodForMeal', 'Music',
    'BestNights', 'HairSpecializesIn', 'DietaryRestrictions'
]


# [[INTERNAL]]
# A parsing function for attributes marked as `boolean_dict_attributes`.
def parse_boolean_dict_attributes(attributes, key, value, include_false):
    value_str = value.replace("'", "\"").replace("u\"", "\"")
    try:
        value_dict = json.loads(value_str)
    except json.JSONDecodeError:
        value_dict = ast.literal_eval(value_str)
    if value_dict is None:
        attributes[key].add(None)
        return
    for value_key, value_value in value_dict.items():
        if not include_false:
            if value_value:
                attributes[key].add(value_key)
        else:
            attributes[key].add((value_key, value_value))


# The main attribute parsing function to convert data frames into attribute dictionaries.
#
# Parameters:
#   - data:          The dataframe to parse.
#   - business_id:   An optional id to focus the attribute scraping to a single business, by string id.
#                    By default, this is None.
#   - include_false: An optional flag to include only attributes marked explicitly in the affirmative.
#   - logger:        An optional named logger to use within this function. If none is provided,
#                    parse_attributes is used.
# Returns:           The fetched attributes dictionary.
def parse_attributes(data, business_id=None, include_false=True, logger=logging.getLogger('parse_attributes')):
    attributes = {}

    # The row parsing function - called once if business_id is not None, called data.shape[0] times otherwise.
    def parse_row(r):
        if r['attributes'] is not None:
            for key, value in r['attributes'].items():
                # Detect new attributes & configure them.
                if key not in attributes:
                    attributes[key] = set()
                if value is None:
                    continue
                match key:
                    case key if key in string_attributes:
                        parse_string_attributes(attributes, key, value, include_false)
                    case key if key in numeric_attributes:
                        parse_numeric_attributes(attributes, key, value, include_false)
                    case key if key in boolean_attributes:
                        parse_boolean_attributes(attributes, key, value, include_false)
                    case key if key in boolean_dict_attributes:
                        parse_boolean_dict_attributes(attributes, key, value, include_false)
                    case _:
                        attributes[key].add(value)

    if business_id is None:
        logger.info(f'Parsing attributes for entire file.')
        data.apply(lambda row: parse_row(row), axis=1)
    else:
        try:
            b_id = data.loc[data['business_id'] == business_id].iloc[0]
            logger.info(f'Finding attributes for business_id: {business_id}')
            parse_row(b_id)
        except IndexError:
            logging.error(f'The business id \'{business_id}\' was not found.', exc_info=True)
    for key in attributes:
        attributes[key] = list(attributes[key])
    return attributes


# A helper function to print an attribute dictionary to the specified file name.
#
# Parameters:
#   - filename:     The relative path to the location where the given attribute dictionary should be written.
#   - attributes:   The attribute dictionary to dump into the file.
def make_attribute_file(filename, attributes):
    with open(filename, 'w') as json_file:
        # Use json.dump to write the dictionary to the file in JSON format
        json.dump(attributes, json_file)
    logging.info(f'Attribute file written to {filename}')


# [[INTERNAL]]
# A test script to validate the attribute dictionary created by the above functions.
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, stream=stdout)
    business_df = load_json_parallel(get_path('business'))
    logging.info(f'Loaded {get_path("business")}')
    attrs = parse_attributes(business_df, include_false=True)
