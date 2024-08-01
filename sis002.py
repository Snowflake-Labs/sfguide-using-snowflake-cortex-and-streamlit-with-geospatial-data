import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import max,min,avg,call_function, split,substr,hour,concat,col,sqrt,lit,array_slice,array_agg,object_construct,parse_json, to_geography, to_array,to_date,round
from snowflake.snowpark.types import StringType,VariantType, DateType,FloatType, IntegerType,DecimalType
import json
import pandas as pd
import numpy as np
import pydeck as pdk
st.set_page_config(layout="wide")
# Write directly to the app
st.title("UK Analytics within the North of England :train:")
st.write(
    """This app shows key insight of places and events that may effect Northern Trains).
    """
)

# Get the current credentials
session = get_active_session()

trains_latlon = session.table('NORTHERN_TRAINS_STATION_DATA.TESTING."StationLatLong"')


#create a point from the coordinates
envelope = trains_latlon.with_column('POINT',call_function('ST_MAKEPOINT',col('"Longitude"'),col('"Latitude"')))

#collect all the points into one row of data
envelope = envelope.select(call_function('ST_COLLECT',col('POINT')).alias('POINTS'))

#create a rectangular shape which boarders the minimum possible size which covers all of the points
envelope = envelope.select(call_function('ST_ENVELOPE',col('POINTS')).alias('BOUNDARY'))

#find the centre point so the map will render from that location

centre = envelope.with_column('CENTROID',call_function('ST_CENTROID',col('BOUNDARY')))
centre = centre.with_column('LON',call_function('ST_X',col('CENTROID')))
centre = centre.with_column('LAT',call_function('ST_Y',col('CENTROID')))

#create LON and LAT variables

centrepd = centre.select('LON','LAT').to_pandas()
LON = centrepd.LON.iloc[0]
LAT = centrepd.LAT.iloc[0]

### transform the data in pandas so the pydeck visualisation tool can view it as a polygon

envelopepd = envelope.to_pandas()
envelopepd["coordinates"] = envelopepd["BOUNDARY"].apply(lambda row: json.loads(row)["coordinates"][0])

places = session.table('OVERTURE_MAPS__PLACES.CARTO.PLACE')
places = places.filter(col('ADDRESSES')['list'][0]['element']['country'] =='GB')

places = places.select(col('NAMES')['primary'].astype(StringType()).alias('NAME'),
                        col('PHONES')['list'][0]['element'].astype(StringType()).alias('PHONE'),
                      col('CATEGORIES')['main'].astype(StringType()).alias('CATEGORY'),
                        col('CATEGORIES')['alternate']['list'][0]['element'].astype(StringType()).alias('ALTERNATE'),
                    col('websites')['list'][0]['element'].astype(StringType()).alias('WEBSITE'),
                      col('GEOMETRY'))

places = places.filter(col('CATEGORY') =='restaurant')

places = places.join(envelope,call_function('ST_WITHIN',places['GEOMETRY'],envelope['boundary']))
places = places.with_column('LON',call_function('ST_X',col('GEOMETRY')))
places = places.with_column('LAT',call_function('ST_Y',col('GEOMETRY')))

placespd = places.to_pandas()

trains_latlon_renamed = trains_latlon

trains_latlon_renamed = trains_latlon_renamed.with_column_renamed('"CrsCode"','NAME')
trains_latlon_renamed = trains_latlon_renamed.with_column_renamed('"Latitude"','LAT')
trains_latlon_renamed = trains_latlon_renamed.with_column_renamed('"Longitude"','LON')

station_info = session.table('BUILD_UK.DATA.TRAIN_STATION_INFORMATION')

trains_latlon_renamed = trains_latlon_renamed.join(station_info,station_info['"CRS Code"']==trains_latlon_renamed['NAME']).drop('"CRS Code"')
trains_latlon_renamed_pd = trains_latlon_renamed.to_pandas()

events = session.table('BUILD_UK.DATA.EVENTS_IN_THE_NORTH')
events = events.join_table_function('flatten',parse_json('EVENT_DATA')).select('VALUE')
events=events.with_column('NAME',col('VALUE')['NAME'].astype(StringType()))
events=events.with_column('DESCRIPTION',col('VALUE')['DESCRIPTION'].astype(StringType()))
events=events.with_column('CENTROID',to_geography(col('VALUE')['CENTROID']))
events=events.with_column('COLOR',col('VALUE')['COLOR'])
events=events.with_column('DATE',col('VALUE')['DATE'].astype(DateType())).drop('VALUE')
events=events.with_column('H3',call_function('H3_POINT_TO_CELL_STRING',col('CENTROID'),lit(5)))

events = events.with_column('R',col('COLOR')[0])
events = events.with_column('G',col('COLOR')[1])
events = events.with_column('B',col('COLOR')[2])
events = events.with_column_renamed('DESCRIPTION','ALTERNATE')
eventspd = events.group_by('H3','NAME','ALTERNATE','R','G','B').count().to_pandas()

incident_table = session.table('BUILD_UK.DATA.INCIDENTS')
flatten = incident_table.select('MP','INCIDENT_TYPE',parse_json('GENERATED_EVENTS').alias('JSON'))
flatten = flatten.join_table_function('FLATTEN',col('JSON')['incidents'])
flatten = flatten.select('MP',col('INCIDENT_TYPE').alias('NAME'),'VALUE')
flatten = flatten.with_column('ALTERNATE',
                                  col('VALUE')['DESCRIPTION_OF_INCIDENTS'].astype(StringType()),
                                 )
flatten = flatten.with_column('LAT',
                                  col('VALUE')['LOCATION']['LAT'].astype(FloatType()))                           
flatten = flatten.with_column('LON',
                                  col('VALUE')['LOCATION']['LON'].astype(FloatType()))
flatten = flatten.with_column('REPORTED_BY',
                                  col('VALUE')['REPORTED_BY'].astype(StringType()),
                                 )
flatten = flatten.with_column('DATE',
                                  col('VALUE')['DATE'].astype(StringType()),
                                 ).drop('VALUE')
flattenpd = flatten.to_pandas()
####MAP LAYERS

polygon_layer = pdk.Layer(
            "PolygonLayer",
            envelopepd,
            opacity=0.3,
            get_polygon="coordinates",
            filled=True,
            get_fill_color=[16, 14, 40],
            auto_highlight=True,
            pickable=False,
        )

 
poi_l = pdk.Layer(
            'ScatterplotLayer',
            data=placespd,
            get_position='[LON, LAT]',
            get_color='[255,255,255]',
            get_radius=600,
            pickable=True)


nw_trains_l = pdk.Layer(
            'ScatterplotLayer',
            data=trains_latlon_renamed_pd,
            get_position='[LON, LAT]',
            get_color='[0,187,2]',
            get_radius=600,
            pickable=True)

h3_events = pdk.Layer(
        "H3HexagonLayer",
        eventspd,
        pickable=True,
        stroked=True,
        filled=True,
        extruded=False,
        get_hexagon="H3",
        get_fill_color=["255-R","255-G","255-B"],
        line_width_min_pixels=2,
        opacity=0.4)



incidents_layer = pdk.Layer(
            'ScatterplotLayer',
            data=flattenpd,
            get_position='[LON, LAT]',
            get_color='[255,165,0]',
            get_radius=2000,
            pickable=True)

#### render the map showing trainstations based on overture maps

tooltip = {
   "html": """<b>Name:</b> {NAME} <br> <b>Alternate:</b> {ALTERNATE}""",
   "style": {
       "width":"50%",
        "backgroundColor": "steelblue",
        "color": "white",
       "text-wrap": "balance"
    }
    }
    
letters = session.table('BUILD_UK.DATA.LETTERS_TO_MP')
mps = letters.select('MP').distinct()
selected_mp = st.selectbox('Choose MP:',mps)
letterspd = letters.filter(col('MP')==lit(selected_mp)).to_pandas()
#st.write(letterspd.PROMPT.iloc[0])

st.divider()
col1,col2 = st.columns([0.5,0.5])

with col1:
    st.markdown('##### MAP OF EVENTS WITH ALL EFFECTED STATIONS AND RESTAURANTS')
    st.pydeck_chart(pdk.Deck(
    map_style=None,
    initial_view_state=pdk.ViewState(
        latitude=LAT,
        longitude=LON,
        zoom=7,
        height=750
        ),
    
    layers= [polygon_layer, poi_l, h3_events,nw_trains_l,incidents_layer ], tooltip = tooltip

    ))
    st.caption('Hover for more info')
with col2:
    st.markdown('#### LETTER TO CHOSEN MP')
    st.write(letterspd.LETTER.iloc[0])
    st.divider()

social_media = session.table('DATA.V_SOCIAL_MEDIA').filter(col('MP')==selected_mp)

st.markdown('##### SOCIAL MEDIA')
st.table(social_media.drop('V'))
