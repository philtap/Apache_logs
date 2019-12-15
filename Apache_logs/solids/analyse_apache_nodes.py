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
# OUT OF OR IN connection WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import pandas as pd

from plotly.express import pd
import plotly.graph_objs as go
import plotly.express as px
from plotly.subplots import make_subplots

from dagster_toolkit.postgres import postgres_warehouse_resource
from dagster import (solid, String, Output, OutputDefinition)
from dagster_pandas import DataFrame
import pycountry

import psycopg2

from db_toolkit.postgres import count_sql


###################################################
# Sessions per hour - bar chart with heat color
####################################################
@solid(required_resource_keys={'postgres_warehouse'})
def graph1_avg_sessions_by_hour(context):

    client = context.resources.postgres_warehouse.get_connection(context)

    if client is not None:
        try:

            cursor = client.cursor()

             # added 13h to get the real customer timezone
            Sessions_by_hour_query = ''' SELECT EXTRACT(HOUR FROM ( session_start_time + interval '13 hour')) session_time  , 
                                        (count(*)/ 7) avg_number_of_sessions
                                        FROM apache_session
                                        GROUP BY session_time;
                        '''
            df = pd.read_sql(Sessions_by_hour_query, client)

            fig = px.bar(df, x='session_time', y='avg_number_of_sessions',
                 labels={'session_time': 'Time of day (in customer timezone)', 'avg_number_of_sessions': 'Average number of sessions'},
                 color='avg_number_of_sessions', title='Sessions by hour')
            fig.show()

        finally:
            # tidy up
            cursor.close()
            client.close_connection()

###################################################
# Sessions per hour - bar chart with heat color
####################################################
@solid(required_resource_keys={'postgres_warehouse'})
def graph2_avg_bookings_by_hour(context):

    client = context.resources.postgres_warehouse.get_connection(context)

    if client is not None:
        try:

            cursor = client.cursor()

             # added 13h to get the real customer timezone
            Bookings_by_hour_query = ''' SELECT EXTRACT(HOUR FROM ( session_start_time + interval '13 hour')) session_time  , 
                                        (count(*)/ 7) avg_number_of_bookings
                                        FROM apache_session
                                        WHERE last_step =6
                                        GROUP BY session_time;
                                    '''
            df = pd.read_sql(Bookings_by_hour_query, client)

            fig = px.bar(df, x='session_time', y='avg_number_of_bookings',
                 labels={'session_time': 'Time of day (in customer timezone)', 'avg_number_of_bookings': 'Average number of Bookings'},
                 color='avg_number_of_bookings', title='Bookings by hour')
            fig.show()

        finally:
            # tidy up
            cursor.close()
            client.close_connection()

@solid(required_resource_keys={'postgres_warehouse'})
def graph3_visitor_bookings_pie_charts(context):
################################################################
# Pie and Stacked pie  - Visitors and Bookings  (mobile vs CUI)
################################################################

    client = context.resources.postgres_warehouse.get_connection(context)

    if client is not None:
        try:
            cursor = client.cursor()
            ##################################################
            #   Visitors  - mobile vs CUI
            ##################################################
            visitors_by_channel_query = '''  SELECT channel, count(*) visitors
                                FROM apache_session
                                GROUP BY channel
                                ; '''

            df1 = pd.read_sql(visitors_by_channel_query, client)

            context.log.info(f' {df1} ')
            x1 = df1['channel']
            y1 = df1['visitors']

            # fig = go.Figure(data=[go.Pie(labels=x1, values=y1)])
            #             # fig.update_traces(hoverinfo='value', textinfo='label+percent', textfont_size=10)
            #             # fig.update_layout(
            #             #     title="Airline website - Visitors by channel",
            #             # )

            # fig.show()

            ##################################################
            #   Bookings  - mobile vs CUI
            ##################################################

            cursor = client.cursor()

             # May include Unknown
            visitors_by_channel_query = '''  SELECT channel, count(*) bookings
                                    FROM apache_session
                                    WHERE last_step = 6
                                    GROUP BY channel
                                    ; '''

            df2 = pd.read_sql(visitors_by_channel_query, client)

            # Debug
            context.log.info(f' {df2} ')

            x2 = df2['channel']
            y2 = df2['bookings']

            # fig = go.Figure(data=[go.Pie(labels=x2, values=y2)])
            # fig.update_traces(hoverinfo='value', textinfo='label+percent', textfont_size=10)
            # fig.update_layout( title="Bookings by channel", )
            # fig.show()

            ###################################################
            # Plot chart with area proportional to total count
            # Visitors and bookings by channel (mobile vs CUI)
            ####################################################

            fig = make_subplots(1, 2, specs=[[{'type': 'domain'}, {'type': 'domain'}]],
                                    subplot_titles=['Visitors', 'Bookings'])
            fig.add_trace(go.Pie(labels=x1, values=y1, scalegroup='one',
                                     name="Visitors"), 1, 1)
            fig.add_trace(go.Pie(labels=x2, values=y2, scalegroup='one',
                                     name="Bookings"), 1, 2)

            fig.update_layout(title_text='Visitors and bookings by channel')
            fig.show()

        finally:
            # tidy up
            cursor.close()
            client.close_connection()


###################################################
# Conversion rate - funnels
###################################################

@solid(required_resource_keys={'postgres_warehouse'})
def graph4_conversion_rate_funnels(context):
    
    def cumulated_count(count_list):
        """  The initial list show the count of users who stopped at a particular step
             For a funnel graph, we need the total number of users who reached each a step , even if they
             continue further
             So we need to cumulate counts
            :param path: initial list of count
            :return: the list with the cumulated count
         """
        length = len(count_list)
        # Calculating the value for each step
        # It is the sum of all the elements after and including the elementt in the initial list
        agg_count_list = [0] * length
        for i in range(length):
            for k in range(i, length):
                agg_count_list[i] = agg_count_list[i] + count_list[k]

        return (agg_count_list)

    
    client = context.resources.postgres_warehouse.get_connection(context)

    if client is not None:
        try:
            cursor = client.cursor()
            
            # Query used by the 3 graphs
            Conversion_query = '''  SELECT     a.channel, 
                                            a.last_step , 
                                            b.step_name ,  
                                            count(*) count_per_step
                                FROM apache_session a 
                                INNER JOIN booking_step b ON a.last_step = b.step_number
                                GROUP BY a.channel, a.last_step, b.step_name 
                                ORDER BY a.channel, a.last_step;
                            '''
            df = pd.read_sql(Conversion_query, client)

            context.log.info(f'Conversion rate - funnels - df')
            context.log.info(f' {df}')
        
            # CUI
            CUI_df = df[['last_step', 'step_name', 'count_per_step']][df['channel'] == 'CUI']
            CUI_last_step_count_list = CUI_df['count_per_step'].tolist()
        
            y1 = CUI_df['step_name'].tolist()
            x1 = cumulated_count(CUI_last_step_count_list)
        
            fig = go.Figure \
                    (
                    go.Funnel
                        (
                        y=y1,
                        x=x1,
                        textposition="inside",
                        textinfo="value+percent initial",
                        marker={"color": ["blue"]}
                        # title={ "text": "Airline website - Mobile Conversion rate"},
                    )
                )
        
            fig.update_layout(title_text='Airline website - CUI channel - Conversion rate')
            fig.show()
        
            ###################################################
            # Conversion rate - Mobile funnel
            ####################################################
        
            # Mobile
        
            Mobile_df = df[['step_name', 'count_per_step']][df['channel'] == 'Mobile']
            Mobile_last_step_count_list = Mobile_df['count_per_step'].tolist()
        
            y2 = Mobile_df['step_name'].tolist()
            x2 = cumulated_count(Mobile_last_step_count_list)
        
            # Prepare the graph
            fig = go.Figure \
                    (
                    go.Funnel
                        (
                        y=y2,
                        x=x2,
                        marker={"color": ["red"]},
                        # title = {"text": "Airline website - Mobile Conversion rate"},
                        textposition="inside",
                        textinfo="value+percent initial",
                    )
                )
        
            fig.update_layout(title_text='Airline website - Mobile channel - Conversion rate')
        
            fig.show()
            ###################################################
            # Conversion rate by channel - stacked funnel
            ####################################################
            # As we do not have data for the Search step for Mobile , the stacked funnel will be starting form the 'Selection' step
        
            x1 = x1[1:len(x1) - 1]
            y1 = y1[1:len(y1) - 1]
        
            fig = go.Figure()
        
            fig.add_trace(go.Funnel(
                name='CUI channel',
                orientation="h",
                y=y1,
                x=x1,
                textposition="inside",
                textinfo="value+percent initial")
            )
        
            fig.add_trace(go.Funnel(
                name='Mobile channel',
                y=y1,
                x=x2,
                textinfo="value+percent initial"))
        
            fig.update_layout(title={'text': 'Airline website - Conversion Rate by channel',
                                     'font_size': 20,
                                     'xanchor': 'center',
                                     'yanchor': 'top'})
        
            fig.show()
            
        finally:
             # tidy up
            cursor.close()
            client.close_connection()

###################################################
# Successful bookings per day - for one week
# Stacked bar chart
####################################################
@solid(required_resource_keys={'postgres_warehouse'})
def graph5_bookings_per_day (context):
    
    client = context.resources.postgres_warehouse.get_connection(context)

    if client is not None:
        try:
            cursor = client.cursor()

            Daily_success_query = '''  SELECT DATE (DATE_TRUNC('day', session_end_time + interval '13 hour' )) as booking_date , Channel, count(*) as count
                                        FROM apache_session
                                        WHERE last_step = 6
                                        GROUP BY booking_date, channel;
                                 '''
            df = pd.read_sql(Daily_success_query, client)
        
            x = df['booking_date']
            y1 = df['count'][df['channel'] == 'CUI']
            y2 = df['count'][df['channel'] == 'Mobile']
        
            fig = px.bar(df, x='booking_date', y='count',
                         labels={'booking_date': 'Booking date', 'count': 'Number of bookings'},
                         hover_data=['channel'], color='channel', title='Bookings per day, by channel')
            fig.show()
        finally:
             # tidy up
            cursor.close()
            client.close_connection()


###################################################
# Average session duration -  bar chart
####################################################
@solid(required_resource_keys={'postgres_warehouse'})
def graph6_session_duration(context):
    client = context.resources.postgres_warehouse.get_connection(context)

    if client is not None:
        try:
            cursor = client.cursor()

            session_duration_query = '''    select b.step_name, a.last_step, 
                                            round( avg(a.session_duration/60 )) avg_session_in_mins 
                                            from apache_session a
                                            INNER JOIN booking_step b ON a.last_step = b.step_number
                                            where a.last_step> 0
                                            group by b.step_name, a.last_step
                                            order by a.last_step
                                 '''
            df = pd.read_sql(session_duration_query, client)

            context.log.info(f' {df} ')

            fig = px.bar(df,
                         x='step_name', y='avg_session_in_mins',
                         labels={'step_name': 'Page reached', 'avg_session_in_mins': 'Average session duration in mins'},
                         hover_data=['avg_session_in_mins'],
                         # color='avg_session_in_mins',
                         title='Average session duration based on last page reached')
            fig.show()

        finally:
            # tidy up
            cursor.close()
            client.close_connection()


###################################################
# Geographic graph
####################################################
@solid(required_resource_keys={'postgres_warehouse'})
def graph7_geo(context):

    # Conversion of alpha2 into alpha3 using pycountry
    def alpha3(alpha2):
        country = pycountry.countries.get(alpha_2=alpha2)
        if country is not None:
            return country.alpha_3
        else:
            return ('NaN')

    client = context.resources.postgres_warehouse.get_connection(context)

    if client is not None:
        try:
            cursor = client.cursor()

            visitors_by_country_query = '''   select country_code, country_name, count(*) number_of_visitors
                                        from apache_session
                                        where continent_code in  ('AS','OC')
                                        and country_code != 'NaN'
                                        group by country_code, country_name
                                        order by count(*) desc;
                                 '''
            df = pd.read_sql(visitors_by_country_query, client)


            def alpha3 (alpha2):
                    country = pycountry.countries.get(alpha_2=alpha2)
                    if country is not None:
                        return country.alpha_3
                    else:
                        return ('NaN')

            if df.empty is False:
                #  Apply the function to the counry_code (2 letter code) column for each row
                fn = lambda row: alpha3(row.country_code)
                alpha3_col = df.apply(fn, axis=1)
                df = df.assign(alpha3_country_code=alpha3_col.values)

            context.log.info(f' {df} ')

            fig = px.scatter_geo(df, locations="alpha3_country_code", color="country_name",
                                 hover_name="country_name", size="number_of_visitors",
                                 projection="natural earth", title='Location of Asian Airline website visitors', scope ='asia'
            )

            fig.show()

        finally:
            # tidy up
            cursor.close()
            client.close_connection()

###################################################
# Geographic graph
####################################################
@solid(required_resource_keys={'postgres_warehouse'})
def graph8_geo(context):
        # Sample plotly code
        gapminder = px.data.gapminder().query("year==2007")
        context.log.info(f' {gapminder} ')
        context.log.info(f' {gapminder.columns} ')

        fig = px.scatter_geo(gapminder, locations="iso_alpha", color="continent",
                         hover_name="country", size="pop",
                         projection="natural earth")
        fig.show()

