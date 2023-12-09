#
#  utils.py
#  General-purpose ingest utilities.
#
#  Carson Rau - Fall 2023
#

# Access the full relative file path for a data file of the given name.
#
# Parameters:
#   - name:      The name of the file whose path will be provided.
#   - directory: A directory relative to the project root. If no directory is provided, the default is /data/.
#                Directories included here as parameters do not need any additional path markers.
#   - is_yelp:   If the file requested is a default file provided by the yelp dataset, a specific prefix must be
#                appended to the file name. By default, this is `True`. If `False`, no prefix will be appended.
#   - is_json:   If the file requested is a json file, this value should be `True` (the default).
#                Otherwise, parquet compressed files will be assumed.
#
# Returns:       The string-version of the relative path to the file.
def get_path(name, directory='data', is_yelp=True, is_json=True):
    file_prefix = 'yelp_academic_dataset_' if is_yelp else ''
    file_type = '.json' if is_json else '.parquet'
    return f'../{directory}/{file_prefix}{name}{file_type}'


def get_image_path(name):
    return f'../img/{name}.png'
