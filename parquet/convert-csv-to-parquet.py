# -*- coding: utf-8 -*-

# WhatIsMyBrowser.com User Agent Database CSV to Parquet converter
#
# This file takes a User Agent Database Dump CSV and converts it to
# a Parquet file format, for use with Google BigQuery and similar software.
#
# See:
# https://developers.whatismybrowser.com/useragents/database/
# https://developers.whatismybrowser.com/api/features/regular-database-updates
#
# This script is still in beta; if you have any feedback,
# problems or success stories, we'd love to know about it.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#

# requirements:
# pandas==1.0.1
# pyarrow==0.16.0

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import sys

# the script takes the input and output file names as arguments    
CSV_FILE = sys.argv[1]
PARQUET_FILE = sys.argv[2]

# -- specify field types
dtypes = {
    'id': 'uint32',

    'user_agent': str,

    'times_seen': 'uint32',

    'simple_software_string': str,
    'simple_sub_description_string': str,
    'simple_operating_platform_string': str,

    'software': str,
    'software_name': str,
    'software_name_code': str,

    'software_version': str, 
    'software_version_full': str, 

    'operating_system': str, 
    'operating_system_name': str, 
    'operating_system_name_code': str, 
    'operating_system_version': str, 
    'operating_system_version_full': str, 
    'operating_system_flavour': str, 
    'operating_system_flavour_code': str, 
    'operating_system_frameworks': str, 

    'operating_platform': str, 
    'operating_platform_code': str, 
    'operating_platform_code_name': str, 
    'operating_platform_vendor_name': str, 

    'software_type': str,
    'software_sub_type': str,
    'software_type_specific': str,

    'hardware_type': str,
    'hardware_sub_type': str,
    'hardware_sub_sub_type': str,
    'hardware_type_specific': str,

    'layout_engine_name': str,
    'layout_engine_version': str,

    'extra_info': str,
    'extra_info_dict': str,
    'capabilities': str,
    'detected_addons': str,
}

date_fields = [
    'first_seen_at',
    'last_seen_at',
    'updated_at',
]


def convert(df):
    df = df.astype(dtypes)
    return df


# Because the CSV is so large, it's necessary to process it chunk by
# chunk, rather than load it all into memory in one hit.
chunksize = 100000

csv_stream = pd.read_csv(CSV_FILE, sep=',', chunksize=chunksize, low_memory=False, parse_dates=date_fields, encoding="utf-8")

for i, chunk in enumerate(csv_stream):
    print("Chunk", i)

    converted = convert(chunk)

    # -- if it's the first chunk, start by setting everything up and creating the parquet file
    if i == 0:            
        # -- create a schema based on the first chunk
        parquet_schema = pa.Table.from_pandas(df=converted).schema

        # -- open (create) a Parquet file for writing
        parquet_writer = pq.ParquetWriter(PARQUET_FILE, parquet_schema, compression='snappy')

    # -- Write that CSV chunk to the parquet file
    table = pa.Table.from_pandas(converted, parquet_schema)
    parquet_writer.write_table(table)

# All finished

# -- close the parquet file
parquet_writer.close()
