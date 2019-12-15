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
##########################################################
#   Craete the required apache tables in postgres
# #########################################################

@solid(required_resource_keys={'postgres_warehouse'})
def create_postgres_tables(context):
    client = context.resources.postgres_warehouse.get_connection(context)

    if client is not None:

        cursor = client.cursor()

        try:

            # Create the booking_step configuration table, containing a name, value pair:
            #  Step number and name for each step in the booking flow
            create_apache_tracking_table_SQL = """CREATE TABLE IF NOT EXISTS apache_tracking
            (
                loaded_file text PRIMARY KEY,
                loaded_date timestamp
            )            """
            cursor.execute(create_apache_tracking_table_SQL)
            client.commit()

            create_bs_table_query = """CREATE TABLE IF NOT EXISTS booking_step (
            step_number integer NOT NULL,
            step_name text NOT NULL,
            CONSTRAINT "PK_booking_step" PRIMARY KEY (step_number)
            )
            """
            cursor.execute(create_bs_table_query)
            client.commit()

            booking_step_dic = {'step_number': [1, 2, 3, 4, 5, 6],
                                'step_name': ['Search', 'Selection', 'Summary', 'Traveler Details', 'Payment',
                                              'Confirmation']
                                }

            booking_step_df = DataFrame(booking_step_dic, columns=['step_number', 'step_name'])

            # Insert the configuration data
            insert_bs_query = """INSERT INTO booking_step(
                                step_number, 
                                step_name)
                                VALUES %s 
                                ON CONFLICT DO NOTHING """
            tuples = [tuple(x) for x in booking_step_df.values]

            cursor.execute(count_sql('booking_step'))
            result = cursor.fetchone()
            pre_len = result[0]

            execute_values(cursor, insert_bs_query, tuples)
            client.commit()

            cursor.execute(count_sql('booking_step'))
            result = cursor.fetchone()
            post_len = result[0]

            context.log.info(f'Inserted {post_len - pre_len} records in the booking_step table')

            # create table

            create_apache_session_table_SQL = '''CREATE TABLE IF NOT EXISTS apache_session (
            id SERIAL PRIMARY KEY,
            ip_address TEXT   NOT NULL,
            session_id TEXT   NOT NULL,
            channel TEXT  NOT NULL,
            session_start_time TIMESTAMP NOT NULL,
            session_end_time TIMESTAMP NOT NULL,
            session_duration INT,     
            first_step SMALLINT,
            last_step SMALLINT,
            num_pages_accessed SMALLINT,
            continent_code TEXT,
            country_code TEXT,
            country_name TEXT,    
            city_name TEXT,
            latitude TEXT,
            longitude TEXT,
            timezone TEXT
            ) '''
            ############################################################################################################
            #   Removed  CONSTRAINT UK_apache_session UNIQUE(ip_address,session_id) due to UK violation
            #   when a session is across 2 days
            #  , CONSTRAINT constraint_name UNIQUE (ip_address, session_id)
            # Also
            ############################################################################################################

            context.log.info(f'{create_apache_session_table_SQL}')
            cursor.execute(create_apache_session_table_SQL)
            client.commit()

            create_apache_index1_SQL =  '''  CREATE INDEX IF NOT EXISTS idx_apache_session_start
                                            ON apache_session(session_start_time)
                                        '''
            context.log.info(f'{create_apache_index1_SQL}')
            cursor.execute(create_apache_index1_SQL)
            client.commit()

            create_apache_index2_SQL = ''' CREATE INDEX IF NOT EXISTS idx_apache_session_step
                                            ON apache_session(last_step)
                                        '''
            context.log.info(f'{create_apache_index2_SQL}')
            cursor.execute(create_apache_index2_SQL)
            client.commit()

        finally:
            # tidy up
            cursor.close()
            client.close_connection()
