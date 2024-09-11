# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import any_value,max,min,avg,call_function, split,substr,hour,concat,col,sqrt,lit,array_slice,array_agg,object_construct,parse_json, to_geography, to_array,to_date
from snowflake.snowpark.types import StringType,VariantType, DateType, IntegerType,DecimalType
import json
import pandas as pd
import numpy as np
import pydeck as pdk

st.set_page_config(layout="wide")

# Write directly to the app

col1,col2,col3 = st.columns([0.2,0.6,0.2])
with col1:
    st.image('https://cdn.prgloo.com/web/NorthernRail/NorthernNewLogo.png')

with col3:
    st.image('https://upload.wikimedia.org/wikipedia/en/thumb/f/f4/Met_Office.svg/1200px-Met_Office.svg.png')
st.title("Northern Trains Weather Data")

st.write(
    """This app shows the weather that may affect Northern Trains).
    """
)

# Get the current credentials
session = get_active_session()

#### add postcode sector to trains
trains_latlon = session.table('NORTHERN_TRAINS_STATION_DATA.TESTING."StationLatLong"')
trains_latlon = trains_latlon.with_column('POSTCODE_SPLIT',split(col('"Postcode"'),lit(' ')))
trains_latlon = trains_latlon.with_column('Postcode_outcode',col('POSTCODE_SPLIT')[0].astype(StringType()))
trains_latlon = trains_latlon.with_column('Postcode_inward_code',col('POSTCODE_SPLIT')[1].astype(StringType()))
trains_latlon = trains_latlon.with_column('Postcode_Sector',concat('POSTCODE_OUTCODE',lit('_'),substr(col('Postcode_inward_code'),1,1)))
trains_latlon = trains_latlon.drop('POSTCODE_SPLIT','POSTCODE_OUTCODE','POSTCODE_INWARD_CODE')


# Add hourly and daily weather for each train station

weather_hourly = session.table('POSTCODE_SECTOR_WEATHER_FORECASTS__ADVANCED_WITH_SOLAR.PCSECT_FORECAST."advanced_with_solar_hourly_view"')
weather_daily = session.table('POSTCODE_SECTOR_WEATHER_FORECASTS__ADVANCED_WITH_SOLAR.PCSECT_FORECAST."advanced_with_solar_daily_view"')

weather_hourly_max = weather_hourly.agg(max('"Issued_at"').alias('MAX'))
weather_hourly = weather_hourly.join(weather_hourly_max,weather_hourly_max['MAX']==weather_hourly['"Issued_at"']).drop('MAX')

weather_daily_max = weather_daily.agg(max('"Issued_at"').alias('MAX'))
weather_daily = weather_daily.join(weather_daily_max,weather_daily_max['MAX']==weather_daily['"Issued_at"']).drop('MAX')

weather_hourly = weather_hourly.join(trains_latlon,trains_latlon['POSTCODE_SECTOR']==weather_hourly['PC_SECT']).drop('PC_SECT')
weather_hourly = weather_hourly.with_column('VALIDITY_DATE',to_date('"Validity_date_and_time"'))

weather_daily_t = weather_daily.join(trains_latlon,trains_latlon['POSTCODE_SECTOR']==weather_daily['PC_SECT']).drop('PC_SECT')








#add h3 column to weather
weather_daily_h3 = weather_daily_t.with_column('H3',call_function('H3_LATLNG_TO_CELL_STRING',col('"Latitude"'),col('"Longitude"'),lit(5)))


#add northern trains boundary box
#create a point from the coordinates
envelope = trains_latlon.with_column('POINT',call_function('ST_MAKEPOINT',col('"Longitude"'),col('"Latitude"')))

#collect all the points into one row of data
envelope = envelope.select(call_function('ST_COLLECT',col('POINT')).alias('POINTS'))

#create a rectangular shape which boarders the minimum possible size which covers all of the points
envelope = envelope.select(call_function('ST_ENVELOPE',col('POINTS')).alias('BOUNDARY'))

# Create a centre point to position the maps
centre = envelope.with_column('CENTROID',call_function('ST_CENTROID',col('BOUNDARY')))
centre = centre.with_column('LON',call_function('ST_X',col('CENTROID')))
centre = centre.with_column('LAT',call_function('ST_Y',col('CENTROID')))



#create LON and LAT variables

centrepd = centre.select('LON','LAT').to_pandas()
LON = centrepd.LON.iloc[0]
LAT = centrepd.LAT.iloc[0]

# index the boundary with h3 res 5
boundary_h3 = envelope.with_column('H3',call_function('H3_COVERAGE_STRINGS',col('BOUNDARY'),lit(5))).join_table_function('flatten',col('H3')).select(col('VALUE').astype(StringType()).alias('H3'))


#### use the postcodes data set to add H3 to postcode sectors
postcodes = weather_daily.group_by('PC_SECT').agg(any_value('POINT').alias('POINT'))
postcodes = postcodes.with_column('H3',call_function('H3_POINT_TO_CELL_STRING',col('"POINT"'),lit(5)))
postcodes = postcodes.join(boundary_h3,boundary_h3['H3']==postcodes['H3'],lsuffix='L').drop('H3L')


### join weather data to the boundary
weather_boundary = boundary_h3.join(weather_daily_h3,weather_daily_h3['H3']==boundary_h3['H3'],lsuffix='L',type='right').drop('H3L')
weather_boundarypd = weather_boundary.select('H3','"Max_temperature_day"','"Max_feels_like_temperature_day"').to_pandas()


from snowflake.snowpark.functions import dateadd


##join weather to all postcode sectors


weather_daily_north = weather_daily.join(weather_daily_max,weather_daily_max['MAX']==weather_daily['"Issued_at"']).drop('MAX')
weather_daily_north = weather_daily_north.join(postcodes,postcodes['PC_SECT']==weather_daily_north['PC_SECT'])







station_filter = trains_latlon.select('"CrsCode"')
date_filter = weather_hourly.agg(max('VALIDITY_DATE').alias('MAX'),
                          min('VALIDITY_DATE').alias('MIN'))

date_filter = weather_hourly.agg(max('VALIDITY_DATE').alias('MAX'),
                          min('VALIDITY_DATE').alias('MIN')).to_pandas()

with st.form('select_data'):
    col1,col2, = st.columns([0.3,0.7])
    with col1:
        selected_station = st.selectbox('Select Station:',station_filter)
        selected_date = st.date_input('Select Date:',date_filter.MIN.iloc[0],date_filter.MIN.iloc[0],date_filter.MAX.iloc[0])
        model = 'reka-flash','mixtral-8x7b', 'gemma-7b','llama2-70b-chat'
        select_model = st.selectbox('Choose Model',model)
        run = st.form_submit_button('Run Dashboard')
    with col2:
        st.image('https://cdn.prgloo.com/media/aad47116f9cc4c33b6f02e51fb8070f4.jpg?width=1200&height=400')

if run:

    #st.write(weather_daily_north).limit(10)
    todaypd = weather_daily_t.filter((col('"Validity_date"')==selected_date)
                                     & (col('"CrsCode"')==selected_station)).to_pandas()
    st.write(todaypd)
    melt = pd.melt(todaypd)
    melt['variable'] = melt['variable'].astype("string")
    melt['value'] = melt['value'].astype("string")

    ##### create an object to feed into the LLM
    object = session.create_dataframe(melt)
    object = object.with_column('object',object_construct(col('"variable"'),col('"value"')))
    
    object = object.select(array_agg('OBJECT').alias('OBJECT'))

    

        ##### create an LLM prompt which includes the data object
    prompt = concat(lit('Summarise the weather report in 500 words using paragraphs for the date specified which includes relevant emojis to summarise the weather based on the following dataset'),
                    col('object').astype(StringType()),
                   lit('USE APPROPIATE MARKDOWN TO ENHANCE THE PRESENTATION. NO COMMENTS'))
    
    # construct the cortex.complete function - this will run based on the chosen model
    complete = call_function('snowflake.cortex.complete',lit(select_model),lit(prompt))
    
    object = object.select(complete)

    #### print the results
    st.write(object.collect()[0][0])

    st.divider()
    
    # dataframes for h3 maps

    weather_daily_north = weather_daily_north.select('H3',col('"Validity_date"').astype(StringType()).alias('DATE'),
                                                     '"Max_temperature_day"',
                                                     '"Max_feels_like_temperature_day"')
    
    weather_daily_north_0 = weather_daily_north.filter(col('"Validity_date"')== dateadd('day',lit(1),lit(selected_date)))
    weather_daily_north_1 = weather_daily_north.filter(col('"Validity_date"')== dateadd('day',lit(2),lit(selected_date)))
    weather_daily_north_2 = weather_daily_north.filter(col('"Validity_date"')== dateadd('day',lit(3),lit(selected_date)))


    ####
    weather_daily_northpd = weather_daily_north_0.to_pandas()
    weather_daily_northpd1 = weather_daily_north_1.to_pandas()
    weather_daily_northpd2 = weather_daily_north_2.to_pandas()

    

    #################CONSTRUCT THE MAP

    ### create a layer to cover the boundary with H3 Indexes

    st.markdown('#### TEMPERATURE COMPARISON OVER THE NEXT 3 DAYS')

    filtered_station = trains_latlon.filter(col('"CrsCode"')==selected_station)
    filtered_station_pd = filtered_station.to_pandas()

    #st.write(filtered_station)



    station = pdk.Layer(
            'ScatterplotLayer',
            data=filtered_station_pd,
            get_position='[Longitude,Latitude]',
            get_color='[0,0,0]',
            get_radius=2000,
            radius_scale=6,
            radius_min_pixels=1,
            opacity=0.6,
            radius_max_pixels=1000,
            filled=True,
            line_width_min_pixels=1,
            pickable=False)
    

    h3_coverage = pdk.Layer(
        "H3HexagonLayer",
        weather_daily_northpd,
        pickable=True,
        stroked=True,
        filled=True,
        extruded=False,
        get_hexagon="H3",
        get_fill_color=["0+Max_temperature_day*8","(255/100) * 0+Max_temperature_day*12","(255/100) * 200-Max_temperature_day*9"],
        line_width_min_pixels=0,
        opacity=0.9)

    h3_coverage_1 = pdk.Layer(
        "H3HexagonLayer",
        weather_daily_northpd1,
        pickable=True,
        stroked=True,
        filled=True,
        extruded=False,
        get_hexagon="H3",
        get_fill_color=["0+Max_temperature_day*8","(255/100) * 0+Max_temperature_day*12","(255/100) * 200-Max_temperature_day*9"],
        line_width_min_pixels=0,
        opacity=0.9)

    h3_coverage_2 = pdk.Layer(
        "H3HexagonLayer",
        weather_daily_northpd2,
        pickable=True,
        stroked=True,
        filled=True,
        extruded=False,
        get_hexagon="H3",
        get_fill_color=["0+Max_temperature_day*8","(255/100) * 0+Max_temperature_day*12","(255/100) * 200-Max_temperature_day*9"],
        line_width_min_pixels=0,
        opacity=0.9)



    #### render the map 


    col1,col2,col3 = st.columns(3)

    with col1:
        st.write(weather_daily_northpd.DATE.iloc[1])
        st.pydeck_chart(pdk.Deck(
        map_style=None,
        initial_view_state=pdk.ViewState(
        latitude=LAT,
        longitude=LON,
        zoom=5,
        height=400
        ),
    
        layers= [h3_coverage,station],tooltip = {'text':"Temperature: {Max_temperature_day}, Feels like: {Max_feels_like_temperature_day}"}

        ))

    with col2:
        st.write(weather_daily_northpd1.DATE.iloc[1])
        st.pydeck_chart(pdk.Deck(
        map_style=None,
        initial_view_state=pdk.ViewState(
            latitude=LAT,
            longitude=LON,
            zoom=5,
            height=400
            ),
    
        layers= [h3_coverage_1,station],tooltip = {'text':"Temperature: {Max_temperature_day}, Feels like: {Max_feels_like_temperature_day}"}

        ))

    with col3:
        st.write(weather_daily_northpd2.DATE.iloc[1])
        st.pydeck_chart(pdk.Deck(
        map_style=None,
        initial_view_state=pdk.ViewState(
        latitude=LAT,
        longitude=LON,
        zoom=5,
        height=400
            ),
    
        layers= [h3_coverage_2,station],tooltip = {'text':"Temperature: {Max_temperature_day}, Feels like: {Max_feels_like_temperature_day}"}

        ))



    
    weather_filter_hour = weather_hourly.filter(col('"CrsCode"') == selected_station)
    weather_filter_day = weather_daily_t.filter(col('"CrsCode"') == selected_station)

    weather_filter_hour = weather_filter_hour.filter(col('VALIDITY_DATE')==selected_date)
    #weather_filter_day = weather_filter_day.filter(col('"Validity_date"')==selected_date)
    weather_filter_hour = weather_filter_hour.with_column('HOUR',hour('"Validity_date_and_time"'))


    weather_filter_hour = weather_filter_hour.group_by('HOUR').agg(avg('"Feels_like_temperature"').alias('Feels Like'),
                                                              avg('"Screen_temperature"').alias('"Screen"'),
                                                               avg('"Screen_Dew_Point_Temperature"').alias('"Dew Point"'),
    avg('"UV_index"').alias(' '),                                                        avg('"Relative_humidity"').alias('"Relative"'),                                                        avg('"Wind_speed"').alias('"Speed"'),
    avg('"Wind_gust"').alias('"Gust"'),).to_pandas()

    #weather_filter_hour
    st.markdown('#### 12 DAY DAILY FORECAST')

    col1,col2 = st.columns(2)
    with col1:
        st.markdown('##### TEMPERATURE')
    #weather_filter_day

        st.line_chart(weather_filter_day.to_pandas(),x='Validity_date',y=['Max_temperature_day','Max_feels_like_temperature_day','Min_feels_like_temperature_night'])


    with col2:
        st.markdown('##### HUMIDITY')
    #weather_filter_day

        st.line_chart(weather_filter_day.to_pandas(),x='Validity_date',y=['Relative_humidity_Approx_Local_Midday','Relative_humidity_Approx_Local_Midnight'])

    st.divider()

    st.markdown('#### HOURLY VIEW')

    col1,col2 = st.columns(2)

    with col1:
        st.markdown('##### TEMPERATURE')
        st.line_chart(weather_filter_hour,x='HOUR',y=['Feels Like','Screen','Dew Point'])
    with col2:
        st.markdown('##### UV INDEX')
        st.bar_chart(weather_filter_hour,x='HOUR',y=[' '])

    with col1:
        st.markdown('##### WIND')
        st.line_chart(weather_filter_hour,x='HOUR',y=['Speed','Gust'])
    with col2:
        st.markdown('##### HUMIDITY')
        st.bar_chart(weather_filter_hour,x='HOUR',y=['Relative'])
