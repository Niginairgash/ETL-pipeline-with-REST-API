import requests
import logging
import pandas as pd
from sqlalchemy import create_engine

#config logging
logging.basicConfig(level=logging.INFO)

def extract_data(api_url):
    logging.info(f"Request data with {api_url}")
    response = requests.get(api_url)

    if response.status_code == 200:
        data = response.json()
        logging.info(f"Data received successfully: {len(data)} events")
        return data
    else:
        logging.error(f"Error while requesting data: {response.status_code}")
        raise Exception(f"API request failed: {response.status_code}")
    

def transform_data(events):
    logging.info("Start data conversion")

    # Transform data to DataFrame
    df = pd.DataFrame(events)

    # Transform data event to DateTime format
    df['event_date'] = pd.to_datetime(df['event_date_utc'], format='ISO8601')

    # Select the required fields
    df = df[['id', 'title', 'event_date', 'details']]

    logging.info("Data converted successfully")
    return df


def load_data_to_db(df, db_connection_string):
    logging.info("Start loading data into the database")

    # Create connection to database
    engine = create_engine(db_connection_string)

    # Load datas to table events
    df.to_sql('events', engine, index=False, if_exists='append')

    logging.info("Data successfully loaded into the database")
