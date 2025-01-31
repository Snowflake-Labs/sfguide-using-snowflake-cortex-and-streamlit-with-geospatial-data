# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, call_function, lit,concat, parse_json,object_construct,replace
from snowflake.snowpark.types import StringType, FloatType, ArrayType, VariantType, DateType



# Write directly to the app


# Get the current credentials
session = get_active_session()

st.markdown(
    """
    <style>
    .heading{
        background-color: rgb(41, 181, 232);  /* light blue background */
        color: white;  /* white text */
        padding: 60px;  /* add padding around the content */
    }
    .tabheading{
        background-color: rgb(41, 181, 232);  /* light blue background */
        color: white;  /* white text */
        padding: 10px;  /* add padding around the content */
    }
    .veh1 {
        color: rgb(125, 68, 207);  /* purple */
    }
    .veh2 {
        color: rgb(212, 91, 144);  /* pink */
    }
    .veh3 {
        color: rgb(255, 159, 54);  /* orange */
    }
    .veh4 {
        padding: 10px;  /* add padding around the content */
        color: rgb(0,53,69);  /* midnight */
    }
    
    body {
        color: rgb(0,53,69);
    }
    
    div[role="tablist"] > div[aria-selected="true"] {
        background-color: rgb(41, 181, 232);
        color: rgb(0,53,69);  /* Change the text color if needed */
    }
    
    </style>
    """,
    unsafe_allow_html=True
)

col1,col2 = st.columns([0.2,0.8])


st.markdown('<h1 class="heading">EVENT SIMULATOR</h2>', unsafe_allow_html=True)
st.write(
    """<BR>Here are all the events and what places are potentially affected again.
    """,unsafe_allow_html=True
)


####Use a higher order filter to filter each array to only show restaurants that are no more than 250m from the event and include this in the LLM

events_what_affected = session.sql('SELECT MP, TRAIN_STATIONS,EVENTS,CASE WHEN ARRAY_SIZE(RESTAURANTS) >10 THEN FILTER(RESTAURANTS,i -> i:DISTANCE_FROM_EVENT <=250) ELSE RESTAURANTS END RESTAURANTS FROM DATA.EVENTS_AND_WHAT_IS_AFFECTED')
mps = events_what_affected.select('MP').to_pandas()

with st.expander("View Prompt Information"):
    st.dataframe(events_what_affected,column_config={
                                    'MP':st.column_config.ListColumn('MP',
                                     help='The current acting MP responsible for the area',
                                     width='medium')   
                                        }
              )

st.markdown('<h4 class="veh1">GENERATE SYNTHETIC EVENTS</h2>', unsafe_allow_html=True)
st.markdown(
    """ 
    Create synthetic events based on a mixture of Activity types. \
    The options are Overcrowding, Food Poisoning, Train Incidents and Fights. <BR><BR>\
    Once complete scrolL down to indidual incidents and wait for social media data to generate

    """,unsafe_allow_html=True)



    
st.image('https://cdn.prgloo.com/media/aad47116f9cc4c33b6f02e51fb8070f4.jpg?width=1200&height=400')



json_template = {"DATE":"01/01/2024",
                 "Location Details":"CRS Code of train station or Restaurant name and address",
                 "LOCATION":{"LAT":0,"LON":0},
                 "REPORTED_BY":"BECKY O'CONNOR","DESCRIPTION_OF_INCIDENTS":"generate unique ficticious incident details here"}

#st.write(json_template)

with st.form('Generate Events'):
    'Generate Synthetic Events based on the following:'
    col1,col2, col3,col4 = st.columns(4)
    with col1:
        model = st.selectbox('Choose Model',['mistral-large2'])
    with col2:
        mp = st.selectbox('Choose MP: ',mps)
    with col3:
        activity = st.selectbox('Choose Activity: ', ['Overcrowding','Food Poisoning','Train Incident','Fight'])
    with col4:
        event_count = st.number_input('How many events:',1,10,5)
    
    submitted = st.form_submit_button('Generate Reports')


if submitted:
    filtered_data = events_what_affected.filter(col('MP')==lit(mp))

    st.markdown('Filtered Objects')

    st.dataframe(filtered_data)

    filtered_data_pd = filtered_data.to_pandas()

    prompt = concat(lit('create'),
                    lit(event_count),
                    lit('synthetic incidents using this json template'),
                    lit(json_template).astype(StringType()), 
                    lit('involving'), 
                    lit(activity), 
                    lit('concerning one of these train stations:'), 
                    col('TRAIN_STATIONS').astype(StringType()), 
                    lit('populate the incident date as the same date as one of these events'),
                    col('EVENTS').astype(StringType()),
                    lit('.Each incident will have connection with the provided event, and will also involve'),
                    lit('one of the following restaurants:'), 
                    col('RESTAURANTS').astype(StringType()),
                    lit('.  Each Incident will be Reported By a synthetic and randomly generated full name'),
                    lit('populate the latitude and longitude as one json element using the provided json template'),
                   lit('Nest all the generated incidents in an single json object called incidents.  Do not include Note'), 
                    lit('RETURN ONLY THE JSON'))

    mistral = call_function('snowflake.cortex.complete',(lit(model),prompt))


    

    generated = filtered_data.with_column('generated_events',mistral)

    generated = generated.with_column('generated_events',replace(col('generated_events'),'''```json''',lit('')))
    generated = generated.with_column('generated_events',replace(col('generated_events'),'''```''',''))
    #st.write(generated)
    generated = generated.select('MP',parse_json('GENERATED_EVENTS').alias('GENERATED_EVENTS'))
    generated = generated.with_column('INCIDENT_TYPE',lit(activity))
    #st.write(generated)

    sql2 = '''create table if not exists DATA.INCIDENTS (MP VARCHAR(255),
            GENERATED_EVENTS VARIANT,
         INCIDENT_TYPE VARCHAR(255))'''
    
    session.sql(sql2).collect()
    generated.write.mode('append').save_as_table("DATA.INCIDENTS")

    

    st.markdown('<h4 class="veh1">NEW EVENTS</h2>', unsafe_allow_html=True)

    st.dataframe(generated)

st.markdown('<h4 class="veh1">GENERATED EVENTS</h2>', unsafe_allow_html=True)


try:
    
    incident_table = session.table('DATA.INCIDENTS')
    st.markdown('<h4 class="veh1">ALL GENERATED EVENTS</h2>', unsafe_allow_html=True)
    
    sql = 'DROP TABLE DATA.INCIDENTS'



    
    clear = st.button('clear incident table')
    
    if clear:
        session.sql(sql).collect()
    
    #st.dataframe(incident_table)

    flatten = incident_table.select('MP','INCIDENT_TYPE',parse_json('GENERATED_EVENTS').alias('JSON'))
    #st.write(flatten)
    flatten = flatten.join_table_function('FLATTEN',col('JSON')['incidents'])
    flatten = flatten.select('MP','INCIDENT_TYPE','VALUE')
    
    flatten = flatten.with_column('DESCRIPTION_OF_INCIDENTS',
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
    
    
    
    st.write(flatten)
    
    
    


    st.divider()

    st.markdown('<h4 class="veh1">INDIVIDUAL INCIDENTS</h2>', unsafe_allow_html=True)
    MP = st.selectbox('Choose MP:',flatten.select('MP').distinct())
    flatten = flatten.filter(col('MP')==MP)
    
    map = flatten.select('LAT','LON')
    st.map(map)
    flattenpd = flatten.to_pandas()
    count = flattenpd.shape[0]
    record = st.slider('Choose Incident Record:',0,count-1,count-1)
    
    st.markdown('###### INCIDENT TYPE')
    st.write(flattenpd.INCIDENT_TYPE.iloc[record])
    st.markdown('###### REPORTED BY')
    st.write(flattenpd.REPORTED_BY.iloc[record])
    st.markdown('###### DATE OF INCIDENT')
    st.write(flattenpd.DATE.iloc[record])
    st.markdown('###### DESCRIPTION OF INCIDENT')
    st.write(flattenpd.DESCRIPTION_OF_INCIDENTS.iloc[record])

    st.divider()

    st.markdown('<h4 class="veh1">NEWLY GENERATED SOCIAL MEDIA</h2>', unsafe_allow_html=True)

    #### SOCIAL MEDIA DATA
    
    social_media = session.create_dataframe([0])
    json = '''{"date","YYYY-MM-DD","post","abcdefg","sentiment_score",0.2,"username","bob"}'''
    
    social_media = social_media.with_column('V',call_function('SNOWFLAKE.CORTEX.COMPLETE',model, 
    concat(lit('generate 4 random synthetic social media post concerning the follwing incident:'), 
    lit(f'''{flattenpd.DESCRIPTION_OF_INCIDENTS.iloc[record]}'''), 
    lit('Add a date, username and relevant emojis to each post.\
    Include emotion.  Return the results as a json object and include sentiment scores.')
           ,lit('use the following json template to structure the data'),lit(json))).astype(VariantType()))
    

    social_media = social_media.with_column('V',replace(col('V'),'''```json''',lit('')))
    social_media = social_media.with_column('V',replace(col('V'),'''```''',''))
    
    smedia = social_media.join_table_function('flatten',parse_json('V')).select('VALUE')
    smedia = smedia.select(object_construct(lit('INCIDENT_TYPE'),lit(flattenpd.INCIDENT_TYPE.iloc[record]),lit('MP'),lit(MP),lit('DATA'),col('VALUE')).alias('V'))
    smedia.write.mode('append').save_as_table('DATA.SOCIAL_MEDIA')
    smedia = smedia.with_column('"Date"',col('V')['DATA']['date'].astype(DateType()))
    smedia = smedia.with_column('"Post"',col('V')['DATA']['post'].astype(StringType()))
    smedia = smedia.with_column('"Sentiment"',col('V')['DATA']['sentiment_score'])
    smedia = smedia.with_column('"Username"',col('V')['DATA']['username'].astype(StringType()))
    smedia = smedia.with_column('"Incident Type"',col('V')['INCIDENT_TYPE'].astype(StringType()))
    smedia = smedia.with_column('"MP"',col('V')['MP'].astype(StringType()))
    st.dataframe(smedia)

    st.divider()
except:
    st.info('No Results Found')
    

    


try:
    st.markdown('<h4 class="veh1">ALL SOCIAL MEDIAL POSTINGS</h2>', unsafe_allow_html=True)
    smediaV = session.table('DATA.SOCIAL_MEDIA')
    smediaV = smediaV.with_column('"Date"',col('V')['DATA']['date'].astype(DateType()))
    smediaV = smediaV.with_column('"Post"',col('V')['DATA']['post'].astype(StringType()))
    smediaV = smediaV.with_column('"Sentiment"',col('V')['DATA']['sentiment_score'])
    smediaV = smediaV.with_column('"Username"',col('V')['DATA']['username'].astype(StringType()))
    smediaV = smediaV.with_column('"Incident Type"',col('V')['INCIDENT_TYPE'].astype(StringType()))
    smediaV = smediaV.with_column('"MP"',col('V')['MP'].astype(StringType()))
    smediaV.create_or_replace_view('DATA.V_SOCIAL_MEDIA')
    st.write(session.table('DATA.V_SOCIAL_MEDIA'))
except:
    st.info('No Results Found')

st.markdown('<h4 class="veh1">WHATS NEXT</h2>', unsafe_allow_html=True)

st.markdown(
    """
    Go to the **ANALYSE_GEO_WITH_CORTEX** notebook to see how Cortex Search can make sense of this generated Unstructured Text

    """,unsafe_allow_html=True)
    
         
