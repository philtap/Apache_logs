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
from os import listdir, path

import numpy as np
import csv
import os
import io
import pandas as pd
import re
import os.path as path
from os import listdir
from os.path import isfile, join
from psycopg2.extras import execute_values

from datetime import *

from dagster import (solid, String, Output, OutputDefinition)
from dagster_pandas import DataFrame

from db_toolkit.postgres import count_sql

##################################################################################
#   1. Extract the apache logs csv file and load its contents to a Pandas dataframe
##################################################################################

#  A subset of the column from the source file are loaded
# The following columns are in the source file but will not be loaded:
# customer, env
# These must be in the csv , they prove that the data is from the correct system (production)
# and for the correct customer (pal)

@solid(
    output_defs=[
        OutputDefinition(dagster_type=String, name='apache_file_name_to_load', is_optional=False),
        OutputDefinition(dagster_type=String, name='apache_file_date', is_optional=False),
        OutputDefinition(dagster_type=DataFrame, name='apache_df', is_optional=False),

    ],
)
def load_apache_csv (context, file_path: String, filename_pattern ):
    """
        Load a csv file into a panda DataFrame
        The solid will scan the directory passed as input for files matching the expected name pattern
        It will take the first matching file that has not already been loaded to postgres
        :param context: execution context
        :param path: path to the folder containing the apache csv files
        :param filename_pattern: the expected format for the apache file names
        :return: panda DataFrame
     """

    # Function to determnine if a file name matches the input file pattern
    def match_pattern (filename, pattern):
        regex = re.compile(pattern)
        match = regex.search(filename)
        if match:
            return True
        else:
            return False

    # Function to extract the date from the csv file
    def apache_file_date (filename):
        if filename is not None:
            return filename[20:31]
        else:
            return 'na'

    # Function to determine if a file was already loaded
    # The input file is a dictionary
    def is_file_loaded (file):
        client = context.resources.postgres_warehouse.get_connection(context)
        if client is not None:
            file_name = file['name']
            cursor = client.cursor()
            try:
                select_apache_tracking_query =  " SELECT COUNT(loaded_file) FROM apache_tracking WHERE loaded_file ='"  +  file_name + "'"

                cursor.execute(select_apache_tracking_query )

                # Return the first value in the tuple
                is_file_loaded = cursor.fetchone()[0]

                if (is_file_loaded == 1):
                    return True
                else:
                    return False

            finally:
                # tidy up
                cursor.close()
                client.close_connection()

    # Start of the load_apache_csv logic
    # Verify the file path exists
    if not path.exists(file_path):
        raise ValueError(f'Invalid directory path: {file_path}')

    # Get the list of files
    all_files = [{'name': f, 'path': join(file_path, f)}
                 for f in listdir(file_path)
                 if isfile(join(file_path, f)) ]

    context.log.info(f' List of files in the directory')
    context.log.info(f' {all_files}')

    # Go through the files in the data directory to find the next file to load
    # load the first file that was not loaded already

    file_name_to_load = 'None'
    file_path_to_load = 'None'

    for f in all_files:
            print('for loop- file:', f)
            filename = f['name']
            file_date = apache_file_date(filename)
            filepath= f['path']
            if match_pattern(filename, filename_pattern):
                    # The curent file in the list is in the correct format
                    # Check if it is already loaded
                    if not is_file_loaded (f):
                        found_a_file=True
                        file_path_to_load= filepath
                        file_name_to_load= filename
                        print('file_to_load=', file_path_to_load)
                        break
            else:
                print(f'The file {filename} does not match the apache file pattern')

    context.log.info(f'The following csv file will be loaded: {file_path_to_load }')

    if file_name_to_load == 'None':
        # Return an empty dataframe if there is no file to load
        df = pd.DataFrame()
        context.log.info(f'There is no apache csv file to load')
        context.log.info(f'Exit the load_apache_csv solid')
    else :
        # If a file is ready to load , read it into a dataframe
        df = pd.read_csv (file_path_to_load ,
                      parse_dates=['@timestamp'],
                      sep=',',
                      usecols=["@timestamp","_id","url_path","referer","geoip.ip","cookie",
                               "user_agent_string","geoip.city_name","geoip.country_code2",
                               "geoip.country_name","geoip.continent_code","geoip.latitude",
                               "geoip.longitude","geoip.timezone","http_method","response_code",
                               "response_size_bytes","response_time_microseconds"])

        df['@timestamp'] = pd.to_datetime (df['@timestamp'])
        context.log.info(f'Loaded {len(df)} records into the apache data frame')

    yield Output(file_name_to_load, 'apache_file_name_to_load')
    yield Output(file_date, 'apache_file_date')
    yield Output(df, 'apache_df')

# ###############################################################
# #  2.  Populate a new session column using the 'cookie' column
# ###############################################################

# This lambda function applies the session_id column to each row in the data frame

@solid
def create_session_col(context,  apache_df, csv_file_date ) -> DataFrame:
    """
            Takes the initial panda DataFrame as input
            The solid will calculate the session_id based on a regular expression on the
            complex cookie column
            :param context: execution context
            :param apache pandas DataFrame
            :return: a dataframe containing one column with the session ID
         """

    # Function takes an element from column cookie and extracts the session_id
    def session_id(one_cookie):
        regexp = r'JSESSIONID=\w{32}'
        if 'JSESSIONID=' in one_cookie:
            # Take the first occurence of the regexp and then take the ID from the string
            session_id_list = re.findall(regexp, one_cookie)
            session_id = session_id_list[0][11:43]
            return session_id
        else:
            # To avoid duplicate issues when loading data for multiple day, unknow values
            # for each day are given a unique value
            return ('Unknown_'+csv_file_date)

    if apache_df.empty is False :
        #  Apply the function to the cookie column for each row
        fn = lambda row: session_id(row.cookie)
        session_col = apache_df.apply(fn, axis=1)

        session_col_df = session_col.to_frame()

        # Return a  dataframe with only the additional session column
        return session_col_df
    else:
        context.log.info(f'There is no apache csv file to process')
        context.log.info(f'Exit the create_session_col solid')
        return pd.DataFrame()


###################################################################################
#   3.  Populate a new accessed_step column using the 'url_path' column
#       This maps the step number with the step in the flow (valid for CUI and mobile)
# ###################################################################################

@solid
def create_accessed_step_col(context, apache_df) -> DataFrame:
    """
            Takes the initial panda DataFrame as input
            The solid will calculate the step of the Booking flow (Mobile or CUI channel)
            corresponding to the URL path of each log entry.
            This is done in 2 steps : calculate the accessed page then the step
            :param context: execution context
            :param apache pandas DataFrame
            :return: a dataframe containing one column the accessed step (1 to 6)
         """
    def accessed_page(one_url_path):
        if ('ApplicationStartAction.do' in one_url_path):
            return 'CUI-Start'
        elif ('AirFareFamiliesForward.do' in one_url_path):
            return 'CUI-AirFareFamilies'
        elif ('AirFareFamiliesFlexibleForward.do' in one_url_path):
            return 'CUI-AirFareFamilies'
        elif ('ItinerarySummary.do' in one_url_path):
            return 'CUI-ItinerarySummary'
        elif ('TravelersDetailsForwardAction.do' in one_url_path):
            return 'CUI-TravelersDetails'
        elif ('PaymentExternal.do' in one_url_path):
            return 'CUI-Payment'
        elif ('PaymentForward.do' in one_url_path):
            return 'CUI-Payment'
        elif ('ConfirmationForward.do' in one_url_path):
            return 'CUI-Confirmation'
        elif ('palmobile/air-shopping/' in one_url_path):
            return 'Mobile-AirFareFamilies'
        elif ('palmobile/cart/' in one_url_path):
            return 'Mobile-ItinerarySummary'
        elif ('palmobile/reservations/' in one_url_path):
            if ('/payment' in one_url_path):
                return 'Mobile-Payment'
            elif ('/confirmation' in one_url_path):
                return 'Mobile-Confirmation'
            else:
                return 'na'
        else:
            return 'na'

    def accessed_step(accessed_page):
        switcher = {
            "CUI-Start": 1,
            "CUI-AirFareFamilies": 2,
            "CUI-ItinerarySummary": 3,
            "CUI-TravelersDetails": 4,
            "CUI-Payment": 5,
            "CUI-Confirmation": 6,
            "Mobile-AirFareFamilies": 2,
            "Mobile-ItinerarySummary": 3,
            "Mobile-Payment": 5,
            "Mobile-Confirmation": 6
        }
        return switcher.get(accessed_page, 0)

    if apache_df.empty is False:
        fn = lambda row: accessed_step(accessed_page(row.url_path))
        accessed_step_col = apache_df.apply(fn, axis=1)
        accessed_step_df = accessed_step_col.to_frame('accessed_step')
        accessed_step_df.reset_index()

        return accessed_step_df

    else:

        context.log.info(f'There is no apache csv file to process')
        context.log.info(f'Exit the create_accessed_step_col solid')
        return pd.DataFrame()



# ########################################################################
# #  4.  Populate a channel column using the 'url_path' column
# ########################################################################

@solid
def create_channel_col (context, apache_df) -> DataFrame:
    """
            Takes the initial panda DataFrame as input
            The solid will calculate the channel (Mobile or CUI )
            corresponding to the URL path of each log entry.
            :param context: execution context
            :param apache pandas DataFrame
            :return: a dataframe containing one column: the channel
         """

    def channel(one_url_path):
        if ('ApplicationStartAction.do' in one_url_path):
            return 'CUI'
        elif ('AirFareFamiliesForward.do' in one_url_path):
            return 'CUI'
        elif ('AirFareFamiliesFlexibleForward.do' in one_url_path):
            return 'CUI'
        elif ('ItinerarySummary.do' in one_url_path):
            return 'CUI'
        elif ('TravelersDetailsForwardAction.do' in one_url_path):
            return 'CUI'
        elif ('PaymentExternal.do' in one_url_path):
            return 'CUI'
        elif ('PaymentForward.do' in one_url_path):
            return 'CUI'
        elif ('ConfirmationForward.do' in one_url_path):
            return 'CUI'
        elif ('palmobile/air-shopping/' in one_url_path):
            return 'Mobile'
        elif ('palmobile/cart/' in one_url_path):
            return 'Mobile'
        elif ('palmobile/reservations/' in one_url_path):
            if ('/payment' in one_url_path):
                return 'Mobile'
            elif ('/confirmation' in one_url_path):
                return 'Mobile'
            else:
                return 'na'
        else:
            return 'Unknown'

    if apache_df.empty is False:

        fn = lambda row: channel(row.url_path)
        channel_col = apache_df.apply(fn, axis=1)
        channel_col_df = channel_col.to_frame()

        return channel_col_df

    else:
        context.log.info(f'There is no apache csv file to process')
        context.log.info(f'Exit the create_channel_col solid')
        return pd.DataFrame()

# ######################################################################################
# #  5.  Add the 3 calculated columns (accessed page not needed) to the Dataframe
# ######################################################################################

@solid
def add_cols_to_df (context, apache_df, session_col_df, channel_col_df, accessed_step_df)-> DataFrame:
    """
            Takes as input the initial panda DataFrame and the 3 dataframes corresponding
            to the derived columns calculated above
            The solid will return the aggregated dataframe
            :param context: execution context
            :param apache pandas DataFrame
            :return: a dataframe containing one column the accessed step (1 to 6)
         """
    if apache_df.empty is False:

        apache_df = apache_df.assign(session=session_col_df.values)
        apache_df = apache_df.assign(channel=channel_col_df.values)
        apache_df = apache_df.assign(accessed_step=accessed_step_df.values)

        return apache_df

    else:

        context.log.info(f'There is no apache csv file to process')
        context.log.info(f'Exit the add_cols_to_df solid')
        return pd.DataFrame()

# ###################################################################################
#   6.  Aggregate the data by session into a new Dataframe
# ###################################################################################
@solid
def aggregate_df_by_session (context, apache_df)-> DataFrame:

    if apache_df.empty is False:
        # Aggregate data per ip_address , session_id
        agg_df = apache_df.groupby(['geoip.ip', 'session'], as_index=True).agg(
        # Keep the group by columns
        ip_address =('geoip.ip', 'first'),
        session_id=('session', 'first'),
        # Get channel for each session
        channel=('channel', 'first'),
        # Get minimum session time for the session
        session_start_time=('@timestamp', min),
        # Get maximum session time for the session
        session_end_time=('@timestamp', max),
        # Get first step reached for the session
        first_step=('accessed_step', min),
        # Get max step reached for the session
        last_step=('accessed_step', max),
        # Total number of pages reached for the session
        num_pages_accessed=('accessed_step', len),
        # Get geographical columns for the session
        continent_code=('geoip.continent_code', 'first'),
        country_code=('geoip.country_code2', 'first'),
        country_name=('geoip.country_name', 'first'),
        city_name=('geoip.city_name', 'first'),
        latitude=('geoip.latitude', 'first'),
        longitude=('geoip.longitude', 'first'),
        timezone=('geoip.timezone', 'first')
        )

        return agg_df

    else:
        context.log.info(f'There is no apache csv file to process')
        context.log.info(f'Exit the aggregate_df_by_session solid')
        return pd.DataFrame()

# ###################################################################################
#   7.  Add derived columns to the Aggregated data frame
# ###################################################################################
@solid
def add_session_duration_col (context, agg_df)-> DataFrame:

    if agg_df.empty is False:
        # Calculate session duration
        fn = lambda row: (row.session_end_time -  row.session_start_time).seconds
        col = agg_df.apply(fn, axis=1)
        agg_df = agg_df.assign(session_duration=col.values)

        return (agg_df)
    else:
        context.log.info(f'There is no apache csv file to process')
        context.log.info(f'Exit the add_session_duration_col solid')
        return pd.DataFrame()

# ###################################################################################
#   8.   Re-organise the Aggregated data frame
# ###################################################################################
@solid
def create_final_df (context, apache_df):

    if apache_df.empty is False:

        # First step, eliminate rows without an IP address as they cannot be used
        apache_df=apache_df [apache_df['ip_address'] != 'NaN']

        # If session_id is Unknown, we will assume that all Unknown session Ids for a day
        # are for the same IP address and correspond to the same session
        # A more sophiosticated approach could be used ( check the timestamp and group
        # in the same sessiion all entries around the same time

        columnsTitles = [
        'ip_address',
        'session_id',
        'channel',
        'session_start_time',
        'session_end_time',
        'session_duration',
        'first_step',
        'last_step',
        'num_pages_accessed',
        'continent_code',
        'country_code',
        'country_name',
        'city_name',
        'latitude',
        'longitude',
        'timezone']
        apache_df.reset_index()
        apache_df = apache_df.reindex(columns=columnsTitles)
        return apache_df

    else:
        context.log.info(f'There is no apache csv file to process')
        context.log.info(f'Exit the create_final_df solid')
        return pd.DataFrame()

@solid(required_resource_keys={'postgres_warehouse'})
def upload_to_postgres(context, final_df, csv_file_name):
    """
    Upload panda DataFrame to Postgres server and update the tracking table
    :param context: execution context
    :param df: DataFrame
    :param csv_file_name: name of the loaded csv file
    :return: None
    :rtype: dict
    """

    if final_df.empty is False:
        client = context.resources.postgres_warehouse.get_connection(context)

        if client is not None:
            try:
                # Insert the final data frame, result of the ETL pipeline, into the apache_session table
                cursor = client.cursor()

                insert_query = """ INSERT INTO apache_session (
                            ip_address,
                            session_id,
                            channel,
                            session_start_time,
                            session_end_time,
                            session_duration,
                            first_step, 
                            last_step, 
                            num_pages_accessed, 
                            continent_code,
                            country_code,
                            country_name, 
                            city_name, 
                            latitude, 
                            longitude, 
                            timezone         
                            ) VALUES %s"""
                tuples = [tuple(x) for x in final_df.values]

                cursor.execute(count_sql('apache_session'))
                result = cursor.fetchone()
                pre_len = result[0]

                execute_values(cursor, insert_query, tuples)
                client.commit()

                cursor.execute(count_sql('apache_session'))
                result = cursor.fetchone()
                post_len = result[0]

                context.log.info(f'Inserted {post_len - pre_len} records in the apache_session table')
                cursor.close()

                # Insert an entry in the tracking table to mark the csv file as loaded
                cursor = client.cursor()

                insert_apache_tracking_sql = """ INSERT INTO apache_tracking
                                    (loaded_file, 
                                    loaded_date
                                    ) VALUES %s
                               """

                dt = datetime.now()
                tracking_tuple = (csv_file_name, dt)

                cursor.execute(insert_apache_tracking_sql , (tracking_tuple,))
                client.commit()

            finally:
                # tidy up
                cursor.close()
                client.close_connection()
        else:
            context.log.info(f'There is no apache csv file to process')
            context.log.info(f'Exit the upload_to_postgres solid')



