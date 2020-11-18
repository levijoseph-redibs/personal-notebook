# imports
import pandas as pd
import numpy as np
import requests
import json
import math
from sqlalchemy import create_engine


# Config
SF_USER = 'LJOSEPH'
SF_PASSWORD = '<>'
ACCOUNT = 'zg83245.east-us-2.azure'
REMINE_URI = 'https://data.remine.com/api/prd/v2/locations'
REMINE_TOKEN = 'Bearer <>' # JB or Levi has this
INPUT_FILE = '/Users/levijoseph/Work/personal-notebook/input/remine/agent_df.csv'
OUTPUT_FOLDER = '/Users/levijoseph/Work/personal-notebook/output/remine'

# query for JPAR agent's
# this query selects those agent IDs whose parent brokers are one of the two JPAR locations
# it then pulls all buyside listings for that agent and pulls the most recent by address
# NOTE: may need to add data elements to pull depending on requirements of API search
# NOTE: future use case will need to ONLY pull minimum set of listings to reduce API expense.
QUERY = '''
    WITH ALL_JPAR_AGENTS as (
      SELECT
          Agent.Member_State_ID as Agent_Member_State_ID

      FROM
          "INGEST_DEV"."REF_DATA"."TREC_CURRENT_TAB" as Agent

      LEFT JOIN
          "INGEST_DEV"."REF_DATA"."TREC_CURRENT_TAB" as Broker
          on Agent.RELATED_BROKER_STATE_ID = Broker.MEMBER_STATE_ID

      WHERE Agent.License_Status in ('Current and Active','Probation and Active') 
          AND Broker.Member_State_ID in 
              (
              'TX|09002729', -- JPAR Dallas - National HQ + DFW)
              'TX|09008344' -- JPAR The Sears Group (Houston Franchise)
                  )
    ),

    ALL_JPAR_AGENT_IDS as (
        SELECT
            Agent_Member_State_ID
        FROM
            ALL_JPAR_AGENTS
    ),

    ORDERED_JPAR_BUY_SIDE_LISTINGS as (
        SELECT 
            ROW_NUMBER() OVER (PARTITION BY NormalizedAddress ORDER BY close_date desc) RN,
            NormalizedAddress,
            listing_fact_id,
            listing_guid   
        FROM
            "DW_DEV"."MAIN"."LISTING_FACT" 
        WHERE
            BUYER_AGENT_STATE_ID in (SELECT * FROM ALL_JPAR_AGENT_IDS)
            OR CO_BUYER_AGENT_STATE_ID in (SELECT * FROM ALL_JPAR_AGENT_IDS)
    ),
  
    JPAR_BUY_SIDE_LISTINGS as (
        SELECT 
        listing.listing_fact_id,
        listing.listing_guid,
        listing.NormalizedAddress,
        property.Street_Number,
        property.Street_Name,
        property.Street_Suffix,
        property.Unit_Number,
        property.City,
        property.State_Province,
        property.postal_code
        
        FROM (SELECT listing_fact_id, listing_guid, NormalizedAddress FROM ORDERED_JPAR_BUY_SIDE_LISTINGS WHERE RN = 1) listing
        
        JOIN "DW_DEV"."MAIN"."PROPERTY_DIM" property
            ON listing.NormalizedAddress = property.Normalized_Address
    )
  
    SELECT listing_fact_id,
      listing_guid,
      NormalizedAddress,
      Street_Number,
      Street_Name,
      Street_Suffix,
      Unit_Number,
      City,
      State_Province,
      Postal_Code 
    FROM JPAR_BUY_SIDE_LISTINGS
    LIMIT 1

'''


def pull_listings_from_db(query):
    '''Pull listings from Snowflake DB based.

    Args:
        query (str): sql query for in-scope listings
    
    Returns:
        dataframe with selected query set
    
    '''
    engine = create_engine(
        'snowflake://{user}:{password}@{account}'.format(
            user=SF_USER,
            password=SF_PASSWORD,
            account=ACCOUNT,
            )
        )
    
    cnxn = engine.connect()

    df = pd.read_sql(query, con=engine)
    print("Pulled listings from DB of length {}".format(len(df)))

    return df

class StringConverter(dict):
    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        return str

    def get(self, default=None):
        return str

def pull_listings_from_disk():
    '''Pull listings from Snowflake DB based.
    
    Returns:
        dataframe from disk
    
    '''
    df = pd.read_csv(INPUT_FILE, converters=StringConverter()).head(100)

    print("Pulled listings from disk of length {}".format(len(df)))
    return df

def unpack_normal_address(n_df):
    '''Pull listings from Snowflake DB based.
    
    Args:
        dataframe with normalized address
    
    Returns:
        dataframe with denormalized address
    
    '''
    n_df = pd.concat([n_df, n_df['NORMALIZEDADDRESS'].str.split(', ', expand=True)], axis=1)

    n_df.rename(columns={0: 'n_line_1', 1: 'n_city', 2: 'n_state_zip'}, inplace=True)

    n_df = pd.concat([n_df, n_df['n_state_zip'].str.split(' ', expand=True)], axis=1)

    n_df.rename(columns={0: 'n_state', 1: 'n_zip'}, inplace=True)

    n_df.drop(['n_state_zip'], axis=1, inplace=True)
    
    return n_df

def hit_remine_api(p_dict):
    '''Unpacks individual listing info and queries API for info.
    
    Args:
        p_dict (dict): dictionary of listing information

    Returns:
        json response from API

    '''
    listing_guid = p_dict['LISTING_GUID']
    line1 = p_dict['n_line_1']
    state = p_dict['n_state']
    postal_code = p_dict['n_zip']
    city = p_dict['n_city']

    # street_name = p_dict['STREET_NAME']
    # street_number = p_dict['STREET_NUMBER']
    # street_suffix = p_dict['STREET_SUFFIX']
    # unit_number = p_dict['UNIT_NUMBER']
    # city = p_dict['CITY']
    # state_province = p_dict['STATE_PROVINCE']
    # postal_code = p_dict['POSTAL_CODE']

    # line1 = " ".join([street_number, street_name, street_suffix])
    # if len(unit_number):
    #     line1 = line1 + " " + unit_number

    # NOTE: Will need to see the documentation on API enable 
    # https://data.remine.com/api/prd/v2/locations/'
    
    head = {'Authorization': REMINE_TOKEN}
    
    uri_search_string = REMINE_URI + '/search?line1={}&state={}&zip={}&line2&city={}'.format(line1, state, postal_code, city) # add search criteria to additional string
    
    print(uri_search_string)

    response = requests.get(uri_search_string, headers = head)

    return listing_guid, response
    

def parse_remine_to_df(r_dict):
    '''Optional: can also write parsing logic in python in order to do ETL here.'''
    pass


def main():
    '''Main function
    
    1. Pulls listings from database
    2. Iterates over listings and hits Remine API
    3. Writes aggregated JSON respons dict to folder
    (4. Writes transformed CSV to folder) <-- may do in Snowflake instead.

    '''
    # listings_df = pull_listings_from_db(QUERY)
    listings_df = pull_listings_from_disk()

    listings_df = unpack_normal_address(listings_df)

    listings_dict = listings_df.to_dict('index')

    results_dict = {}
    
    l_counter = 0
    s_counter = 0
    counter = len(listings_dict.keys())
    for listing in listings_dict.values():
        listing_guid, owner_info = hit_remine_api(listing)
        
        l_counter += 1

        if owner_info.status_code == 200:
            s_counter += 1
            print("SUCCESS!!! {} success rate with {} done".format(s_counter/counter, l_counter/counter)) 
            
            response_dict = json.loads(owner_info.content.decode("UTF-8"))
            results_dict[listing_guid] = response_dict
        else:
            print("failure...{}".format(owner_info.status_code))
        

    # NOTE: Could also send this to Azure blob location.
    json_output = OUTPUT_FOLDER + '/remine_results.json'
    with open(json_output, 'w') as outfile:
        json.dump(results_dict, outfile, sort_keys=True, indent=4)
    
    # remine_df = parse_remine_to_df(results_dict)
    # csv_output = OUTPUT_FOLDER + '/remine_results.csv'
    # remine_df.to_csv(csv_output)
    

if __name__ == '__main__':
    # execute only if run as a script
    main()