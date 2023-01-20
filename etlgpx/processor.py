import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import os
import gpxpy 
from gpxpy.gpx import GPXTrack
from typing import List, Dict
from dotenv import load_dotenv


class Processor:
    def __init__(self, data_path: str = None) -> List[GPXTrack]:
        self.data_path = data_path
        self.data = None
        self.transformed_data = None
        self.create_table_query = 'CREATE TABLE IF NOT EXISTS gpx_tracks (' + \
                                  'id INT PRIMARY KEY IDENTITY(1, 1),' + \
                                  'time DATETIME,' + \
                                  'latitude FLOAT,' + \
                                  'longitude FLOAT,' + \
                                  'speed FLOAT,' + \
                                  'course FLOAT);'

    def extract(self, data_path: str = None):
        if data_path:
            self.data_path = data_path
        
        if self.data_path is None:
            raise ValueError('Data path is not set up')

        with open(self.data_path, 'r') as gpx_file:  
            gpx = gpxpy.parse(gpx_file, version='1.1')
            self.data = gpx.tracks
        return self.data

    def transform(self, data: List[GPXTrack]=None) -> pd.DataFrame:
        points = []
        if data:
            self.data = data
        
        for track in self.data:
            for segment in track.segments:
                for p in segment.points:
                    speed = None 
                    course = None
                    for c in p.extensions[0]:
                        if 'speed' in c.tag:
                            speed = float(c.text)
                        elif 'course' in c.tag:
                            course = float(c.text)

                    points.append({
                        'time': p.time,
                        'latitude': p.latitude,
                        'longitude': p.longitude,
                        'speed': speed,
                        'course': course
                    })
        
        self.transformed_data = pd.DataFrame(points)
        return self.transformed_data

    def load(self,
             data: pd.DataFrame=None,
             create_table_query: str=None):
        if data is not None:
            if isinstance(data, pd.DataFrame):
                self.transformed_data = data
            else:
                raise ValueError('Data need to be provided as pandas DataFrame')
        load_dotenv()
        config = {
            'dbname': os.getenv('REDSHIFT_DB_NAME'),
            'host': os.getenv('REDSHIFT_HOST'),
            'port': int(os.getenv('REDSHIFT_PORT')),
            'user': os.getenv('REDSHIFT_USER'),
            'password': os.getenv('REDSHIFT_PASSWORD'),
        }

        try:
            db = create_engine(self._prepare_conn_string(config))
            df_conn = db.connect()
            conn = psycopg2.connect(**config)
            cur = conn.cursor()
            if create_table_query:
                self.create_table_query = create_table_query
            self._create_table_if_not_exists(conn)
            print("TABLE CREATED IF NOT EXISTED")
            self.transformed_data.to_sql(os.getenv('REDSHIFT_TABLE'),
                                         df_conn,
                                         if_exists='replace',
                                         index=False)
            print("DATA SAVED ON REDSHIFT CLUSTER")
            cur.close()
            conn.close() 
            df_conn.close()

        except psycopg2.Error as e:
            print(e)

    def _create_table_if_not_exists(self,
                                    conn) -> None:
        try:
            cur = conn.cursor()
            cur.execute(self.create_table_query)
            conn.commit()
            cur.close()
        except psycopg2.Error as e:
            print(e)
    
    def _prepare_conn_string(self, config: Dict[str, int]) -> str:
        return f'postgresql://{config["user"]}:{config["password"]}' + \
               f'@{config["host"]}:{config["port"]}/{config["dbname"]}'


if __name__ == '__main__':
    p = Processor()
    p.extract('/home/pito/gpx-tracks-etl/data/3a6579b2934311edbd18139731566f23.gpx')
    p.transform()
    p.load()