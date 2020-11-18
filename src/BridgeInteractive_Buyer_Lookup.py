# imports
import pandas as pd
import numpy as np
import requests
import json
from sqlalchemy import create_engine


# Config
## Input
SF_USER = "LJOSEPH"
SF_PASSWORD = "<>
ACCOUNT = "zg83245.east-us-2.azure"
DATABASE = "INGEST_DEV"
SCHEMA = "AGG_BRIDGE"
WAREHOUSE = "INGEST_DEV_WH"
LIMIT = 1000
## output
OUTPUT_FILE = "/Users/levijoseph/Work/personal-notebook/output/BridgeInteractive_Buyer_Lookup.csv"

# Column constants
## Snowflake
PARCEL_NUMBER = "parcel_number"
LONGITUDE = "longititude"
LATITUDE = "latittude"
SF_INPUT_TAB = "property_current_tab"
SF_INPUT_TAB_COLS = ", ".join([PARCEL_NUMBER, LONGITUDE, LATITUDE])

# Bridge Interactive
BI_PARCELS = "parcels"
BI_BUYER_NAME = 
BI_ID = "id"
BI_PARCEL_ID = "parcelID"
BI_FULL = 
BI_HOUSE = 
BI_STREET = 
BI_CITY = 
BI_STATE = 
BI_ZIP = 
BI_COORDINATES = 

# Output
BUYER_NAME = 
ID = 
PARCEL_ID = 
HOUSE = 
STREET = 
CITY = 
STATE = 
ZIP = 
COORDINATES = 



def sample_lookup_properties(engine):
    engine = create_engine(
        'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}'.format(
            user=SF_USER,
            password=SF_PASSWORD,
            account=ACCOUNT,
            database=DATABASE,
            schema=SCHEMA,
            warehouse=WAREHOUSE
            )
        )
    
    cnxn = engine.connect()
    
    # The following could be handled on the DW side through a union and one query
    # Austin
    sql_1 = "select {} from {} where CITY = '{}' limit {}".format(SF_INPUT_TAB_COLS, SF_INPUT_TAB, 'Austin', LIMIT)
    df_1 = pd.read_sql(sql_1, con=engine)

    # Dallas
    sql_2 = "select {} from {} where CITY = '{}' limit {}".format(SF_INPUT_TAB_COLS, SF_INPUT_TAB, 'Dallas', LIMIT)
    df_2 = pd.read_sql(sql_2, con=engine)

    # Houston
    sql_3 = "select {} from {} where CITY = '{}' limit {}".format(SF_INPUT_TAB_COLS, SF_INPUT_TAB, 'Houston', LIMIT)
    df_3 = pd.read_sql(sql_3, con=engine)

    # Concetenate into one query
    prop_df = df_1.append([df_2, df_3])

    # Confirm the df is the right length
    assert len(prop_df) == LIMIT * 3

    return prop_df
    

def get_owner_from_parcel():


def get_parcel_from_long_lat(lo, la, token=TOKEN):
    lo_la = ",".join([lo,la])
    response = requests.get("https://api.bridgedataoutput.com/api/v2/pub/parcels?access_token={}&near={}".format(token, lo_la))
    response_dict = json.loads(response.content.decode("UTF-8"))
    payload = response_dict['bundle']
    
    for load in payload:
        if len(load['coordinates']) > 0:
            lo_f = load['coordinates'][0]
            la_f = load['coordinates'][1]
        
            if (lo_f == float(long)) & (la_f == float(lat)):
                print("FOUND!!!")
                return load['transactionsUrl']
            else:
                print("not found.")
                return None
        else:
            return None


def parse_parcel_payload()


def prepare_prop_owner_df(prop_owner_dict):



main():
    '''lookup properties to return buyer'''
    
    # sample lookup properties
    prop_df = sample_lookup_properties()

    prop_dict = prop_df.to_dict('index')
    prop_owner_dict = {}

    for index, prop in prop_dict.items():
        parcel = prop['parcel_number']
        lo = prop['longitude']
        la = prop['latitude']

        payload = get_owner_from_parcel()
            
        if payload is None:
            payload = get_parcel_from_long_lat(lo, la)
        
        parsed_parcel_payload = parse_parcel_payload(payload)

        prop_owner_dict[index] = parsed_parcel_payload


    prop_owner_df = prepare_prop_owner_df(prop_owner_dict)
    prop_owner_df.to_csv(OUTPUT_FILE)

if __name__ == "__main__":
    # execute only if run as a script
    main()