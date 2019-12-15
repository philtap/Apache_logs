# The MIT License (MIT)
# Copyright (c) 2019 Philippe Tap

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
# from os import listdir, path

# import numpy as np
# import csv
# import os
# import io
# import pandas as pd
# import re
# import os.path as path
# from os import listdir
# from os.path import isfile, join

from psycopg2.extras import execute_values

from dagster import (solid, String, Output, OutputDefinition)
from dagster_pandas import DataFrame

from db_toolkit.postgres import count_sql
@solid(required_resource_keys={'postgres_warehouse'})
def drop_postgres_tables(context):
    client = context.resources.postgres_warehouse.get_connection(context)

    if client is not None:

        cursor = client.cursor()

        try:

            drop_apache_tracking_table_SQL = """DROP TABLE IF EXISTS apache_tracking"""
            context.log.info(f'{drop_apache_tracking_table_SQL}')
            cursor.execute(drop_apache_tracking_table_SQL)
            client.commit()

            drop_booking_step_table_SQL= """DROP TABLE IF EXISTS booking_step"""
            context.log.info(f'{drop_booking_step_table_SQL}')
            cursor.execute(drop_booking_step_table_SQL)
            client.commit()

            drop_apache_session_table_SQL = '''DROP TABLE IF EXISTS apache_session  '''
            context.log.info(f'{drop_apache_session_table_SQL}')
            cursor.execute(drop_apache_session_table_SQL)
            client.commit()

        finally:
            # tidy up
            cursor.close()
            client.close_connection()