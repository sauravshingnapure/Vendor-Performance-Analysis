import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time 

logging.basicConfig(
    filename='logs/ingestion_db.logs',
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode='a'
)

# Create the SQLite engine
engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    '''This function will ingest the dataframe into database table'''
    df.to_sql(table_name, con=engine, if_exists='append', index=False)

folder = r'C:/Users/asus/OneDrive/Desktop/SAS/saurav/Project/End to end vendor performance/data'

# Process each CSV file in the folder
def load_raw_data():
    '''This function will load the CSVs as dataframe and ingest into db'''
    start=time.time()
    for file in os.listdir(folder):
        if file.endswith('.csv'):
            file_path = os.path.join(folder, file)

            # Read the CSV in chunks
            chunksize = 1000000  # Adjust as needed

            for df in pd.read_csv(file_path, chunksize=chunksize):
                logging.info (f'Ingesting {file} in db')
                ingest_db(df, file[:-4], engine)
    end=time.time()
    total_time= (end-start)/60
    logging.info('-----------Ingestion Complete-----------')
    logging.info(f'Total time taken: {total_time} minutes')
    
if __name__ == '__main__':
    load_raw_data()