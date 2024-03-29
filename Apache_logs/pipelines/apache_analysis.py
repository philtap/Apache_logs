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

from dagster_toolkit.postgres import postgres_warehouse_resource

from db_toolkit.misc.get_env import get_file_path

from Apache_logs.solids import  graph1_avg_sessions_by_hour, graph2_avg_bookings_by_hour,  \
    graph3_visitor_bookings_pie_charts, graph4_conversion_rate_funnels, \
    graph6_session_duration, graph7_geo, graph5_bookings_per_day


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

def postgres_to_visualisation_pipeline():

    graph1_avg_sessions_by_hour()
    graph2_avg_bookings_by_hour()
    graph5_bookings_per_day()
    graph3_visitor_bookings_pie_charts()
    graph4_conversion_rate_funnels()
    graph6_session_duration()
    graph7_geo()


def call_postgres_to_visualisation_pipeline():

    # get path to postgres config file
    postgres_cfg = get_file_path('POSTGRES_CFG', 'Postgres configuration file')
    if postgres_cfg is None:
        exit(0)

    # resource entries for environment_dict
    postgres_warehouse = {'config': {'postgres_cfg': postgres_cfg}}

    def execute_postgres_to_visualisation_pipeline():
        """
        Describe
        """
        # environment dictionary
        postgres_to_visualisation_env_dict = {
            'solids': {
                'graph1_avg_sessions_by_hour':
                    {
                    },
                'graph2_avg_bookings_by_hour':
                    {
                    },
                'graph3_visitor_bookings_pie_charts':
                    {
                    },
                'graph4_conversion_rate_funnels':
                    {
                    },
                'graph5_bookings_per_day':
                    {
                    },
                'graph6_session_duration':
                    {
                    },
                 'graph7_geo':
                     {
                     },
            },
            'resources': {
                'postgres_warehouse': postgres_warehouse,
            }
        }

        result = execute_pipeline(postgres_to_visualisation_pipeline, environment_dict=postgres_to_visualisation_env_dict)
        assert result.success

    execute_postgres_to_visualisation_pipeline()

if __name__ == '__main__':
    # Normal flow: Call the function to execute the pipeline to load the csv file to postgres
    call_postgres_to_visualisation_pipeline()


