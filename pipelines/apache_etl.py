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

from dagster import (
    execute_pipeline,
    pipeline,
    ModeDefinition
)

from db_toolkit.misc.get_env import get_file_path, get_dir_path

from solids.drop_apache_tables_nodes import drop_postgres_tables
from solids.create_apache_tables_nodes import create_postgres_tables


from solids.load_apache_csv_nodes import create_accessed_step_col, create_channel_col, create_session_col, \
    add_cols_to_df, \
    aggregate_df_by_session, add_session_duration_col, create_final_df, upload_to_postgres, load_apache_csv,\
    upload_temp_to_postgres



from dagster_toolkit.postgres import postgres_warehouse_resource


@pipeline(
    mode_defs=[
        ModeDefinition(
            # attach resources to pipeline
            resource_defs={
                'postgres_warehouse': postgres_warehouse_resource,
            }
        )
    ]
)

def create_postgres_tables_pipeline():
    create_postgres_tables()

@pipeline(
    mode_defs=[
        ModeDefinition(
            # attach resources to pipeline
            resource_defs={
                'postgres_warehouse': postgres_warehouse_resource,
            }
        )
    ]
)

def csv_to_postgres_pipeline():

    # Load the first available apache csv file to a pandas dataframe
    # The data frame contains log entries
    # The date and name of the file to load are returned also
    csv_file_name_to_load, csv_file_date , df = load_apache_csv()

    # Calculate new columns
    session_col_df=create_session_col(df, csv_file_date )
    accessed_step_df=create_accessed_step_col(df)
    channel_col_df=create_channel_col(df)

    # Add new columns to the data frame
    extended_df=add_cols_to_df(df, session_col_df, channel_col_df, accessed_step_df)

    # Aggegate the data by session
    agg_df=aggregate_df_by_session(extended_df)

    # Add a column with the duration of each session
    agg_df=add_session_duration_col(agg_df)

    # Prepare the final data frame
    agg_df=create_final_df(agg_df)

    # Upload to the apache_session table in posgtres
    upload_to_postgres(agg_df, csv_file_name_to_load)

def call_create_postgres_tables_pipeline():

    # get path to postgres config file
    postgres_cfg = get_file_path('POSTGRES_CFG', 'Postgres configuration file')
    if postgres_cfg is None:
        exit(0)

    # resource entries for environment_dict
    postgres_warehouse = {'config': {'postgres_cfg': postgres_cfg}}

    def execute_create_postgres_tables_pipeline():
            """
            Execute the pipeline to retrieve the data from the apache csv file(s), apply ETL and save the result to Postgres
            """
            # environment dictionary
            create_tables_pipeline_env_dict = {
                'solids':   {
                            'create_postgres_tables':
                                {
                                }
                            },
                'resources': {
                                'postgres_warehouse': postgres_warehouse,
                            }
            }
            result = execute_pipeline(create_postgres_tables_pipeline, environment_dict=create_tables_pipeline_env_dict)
            assert result.success

    execute_create_postgres_tables_pipeline()

def call_csv_to_postgres_pipeline():

    # get path to the apache logs csv files
    filepath = get_dir_path('CSV_DIR_PATH', 'Apache Csv directory path')
    if filepath is None:
       exit(0)

    filename_pattern = r'apache_access-p-pal-\d{4}.\d{2}.\d{2}.csv'

    # get path to postgres config file
    postgres_cfg = get_file_path('POSTGRES_CFG', 'Postgres configuration file')
    if postgres_cfg is None:
        exit(0)

    # resource entries for environment_dict
    postgres_warehouse = {'config': {'postgres_cfg': postgres_cfg}}

    if filepath is not None:

        def execute_csv_to_postgres_pipeline():
            """
            Execute the pipeline to retrieve the data from the apache csv file(s), apply ETL and save the result to Postgres
            """
            # environment dictionary
            csv_to_postgres_env_dict = {
                'solids':   {
                            'load_apache_csv':
                                {
                                    'inputs':
                                        {
                                            'file_path' : {'value': filepath },
                                            'filename_pattern': {'value': filename_pattern },
                                        }
                                },
                            'create_session_col':
                                 {
                                 },
                            'create_accessed_step_col':
                                {
                                },
                            'create_channel_col':
                                {
                                },
                            'add_cols_to_df':
                                {
                                },
                            'aggregate_df_by_session':
                                {
                                },
                            'add_session_duration_col':
                                {
                                },
                            'create_final_df':
                                {
                                },
                            'upload_to_postgres':
                                {
                                }
                            },
                'resources': {
                                'postgres_warehouse': postgres_warehouse,
                            }
            }
            result = execute_pipeline(csv_to_postgres_pipeline, environment_dict=csv_to_postgres_env_dict)
            assert result.success

        execute_csv_to_postgres_pipeline()

def send_all_files_to_csv_postgres_pipeline():

    # Call the pipeline iteratively to load all the csv files to postgres
     for file in range(7):
        print ('Running csv_to_postgres_pipeline - Iteration: file number: ', file )
        call_csv_to_postgres_pipeline()

if __name__ == '__main__':

    # Call the create table pipeline which creates the tables
    call_create_postgres_tables_pipeline()

    # Normal flow: Call the pipeline to load the csv file to postgres
    call_csv_to_postgres_pipeline()

    # Call the pipeline iteratively to load all the csv files to postgres
    send_all_files_to_csv_postgres_pipeline()



