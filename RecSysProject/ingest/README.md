# /ingest/

A collection of utility scripts for importing and selecting various components
from the various Yelp Dataset JSON files.

## Table of Contents

Each file has an associated internal file containing helper functions and other utilities. These files are suffixed
with an underscore. Their exports should not be used outside the main file with the same name. These files are widely
undocumented as functions and declarations here have limited use elsewhere.

### hours_ingest.py
A script enabling the parsing of the `hours` column within the yelp_academic_dataset_business.json. This column is a 
nested dictionary, with optional days mapped to a compound string containing both open and closing time. This script's
main export -- `parse_hours()` can be used to create a DataFrame containing columns for both the open and closing time
each day. The ``extract_hours_minutes()`` function can be used to further parse these time strings into proper numeric
values.

Provided Functions:
* ``parse_hours()``
* ``extract_hours_minutes()``

### categories_ingest.py
A script enabling the parsing of the `categories` column within the yelp_academic_dataset_business.json.
This script can be accessed using `parse_categories()`. See function documentation for specific details; however, 
it is of note that both single businesses and entire dataframes can be parsed with this function. 

The resulting dataframe will include all existing columns in the entire dataframe -- even if they are not mentioned on 
the specific row requested.

Provided Functions:
* ``parse_categories()``

### json_ingest.py
The core JSON loading script. This script contains utility functions for performing both serial and parallel loading
of JSON files.

Provided Functions:
* ``load_json()``
* ``load_json_parallel()``

### parquet_ingest.py
This script provides the mechanisms to load and store dataframes to parquet files.

Provided Functions:
* ``parquet_read()``
* ``parquet_write()``

### utils.py
This script includes general-purpose ingest helpers. This includes a path search function to ease working with 
.json/.parquet file paths within this repository.

Provided Functions:
* ``get_path()``
