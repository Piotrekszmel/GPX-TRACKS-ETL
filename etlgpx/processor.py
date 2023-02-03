import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import uuid
import psycopg2
import os
import gpxpy 
from gpxpy.gpx import GPXTrack
from psycopg2.extensions import connection
from typing import List, Dict
from scipy import stats
from global_land_mask import globe
from geopy.geocoders import Nominatim
from typing import Tuple


class Processor:
    def __init__(self, data_path: str = None) -> None:
        """
        Initializes Processor class.

        Args:
            data_path: Path to location with gpx files that will be then
                       extracted while running extract() method.

        Returns:
            None 
        """
        self.data_path = data_path
        self.data = []
        self.transformed_data = pd.DataFrame()
        self.create_table_query = os.getenv("CREATE_TABLE", None)
        self.geolocator = Nominatim(user_agent="geoapiExercises")
        self._config = {
            'dbname': os.getenv('REDSHIFT_DB_NAME'),
            'host': os.getenv('REDSHIFT_HOST'),
            'port': int(os.getenv('REDSHIFT_PORT')),
            'user': os.getenv('REDSHIFT_USER'),
            'password': os.getenv('REDSHIFT_PASSWORD'),
        }

    def extract(self, data_path: str = None) -> List[GPXTrack]:
        """
        Extracts data from gpx files from given location. 
        If location is not provided, then data_path specified
        during Processor initialization will be used. 

        Args:
            data_path: Path to location with gpx files 
                       that will extracted. 
        Returns:
            List of GPXTracks.
        """
        if data_path:
            self.data_path = data_path
        
        if self.data_path is None:
            raise ValueError('Data path is not set up.')

        if self.data_path.endswith('.gpx'):
            with open(self.data_path, 'r') as gpx_file:     
                gpx = gpxpy.parse(gpx_file, version='1.1')
                self.data = gpx.tracks
        else:
            self.data = []
            files = [file for file in os.listdir(self.data_path) 
                     if os.path.isfile(os.path.join(self.data_path, file))]
            for file in files:
                with open(self.data_path + '/' + file, 'r') as gpx_file:
                    gpx = gpxpy.parse(gpx_file, version='1.1')
                    self.data += gpx.tracks
        return self.data

    def transform(self, data: List[GPXTrack]=None) -> pd.DataFrame:
        """
        Transforms data from gpx format into pandas Dataframe. 
        If data is not provided, then data from extract()
        will be used.

        Args:
            data: List of GPXTracks that will be 
                  transformed into pandas Dataframe.
        Returns:
            Pandas DataFrame with transformed data. 
        """
        tracks = []
        if data is not None:
            self.data = data
        elif not self.data: 
            raise ValueError('Data was not provided.')

        for track in self.data:
            points = []
            for segment in track.segments:
                for p in segment.points:
                    speed = None 
                    course = None
                    for c in p.extensions[0]:
                        if 'speed' in c.tag:
                            speed = float(c.text)
                        if 'course' in c.tag:
                            course = float(c.text)
                    if not globe.is_land(p.latitude, p.longitude):
                        points.append({
                            'time': p.time,
                            'latitude': p.latitude,
                            'longitude': p.longitude,
                            'speed': speed,
                            'course': course
                        })
            
            origin_city, dest_city = 'Unknown', 'Unknown'
            if points:
                origin_city, dest_city = self._get_start_and_destination_city(str(points[0]['latitude']),
                                                                              str(points[0]['longitude']),
                                                                              str(points[-1]['latitude']),
                                                                              str(points[-1]['longitude']))
            tracks.append({
                'origin_city': origin_city,
                'dest_city': dest_city,
                'points': points
            })
        
        dfs = []
        for row in tracks:
            origin_city = row['origin_city']
            dest_city = row['dest_city']
            points = row['points']
            df = pd.DataFrame(points)
            df['origin_city'] = origin_city
            df['dest_city'] = dest_city
            df['track_id'] = uuid.uuid4().hex
            dfs.append(df)
        self.transformed_data = pd.concat(dfs, ignore_index=True)
        self._remove_anomalies()
        return self.transformed_data

    def load(self,
             data: pd.DataFrame=None,
             create_table_query: str=None) -> None:
        """
        Loads data into redshift table based 
        on specified configuration. 
        If data is not provided, then data created from transform()
        method will be used.

        Args:
            data: Pandas Dataframe.
            create_table_query: Path to sql file that will 
                                be used to create table.
        Returns:
            None 
        """
        if data is not None:
            if isinstance(data, pd.DataFrame):
                self.transformed_data = data
            else:
                raise ValueError('Data needs to be provided as pandas DataFrame')
        if self.transformed_data.empty:
            raise ValueError('Transformed data was not provided.')
        
        try:
            with psycopg2.connect(**self._config) as conn:
                if create_table_query:
                    self.create_table_query = create_table_query
                if self.create_table_query is not None:
                    self.execute(conn, self.create_table_query, True)
                    print("TABLE CREATED IF NOT EXISTED.")
            
            db = create_engine(self._prepare_conn_string(self._config))
            with db.connect() as conn:
                self.transformed_data.to_sql(os.getenv('REDSHIFT_TABLE'),
                                             conn,
                                             if_exists='append',
                                             index=False,
                                             method='multi')
                print("DATA SAVED ON REDSHIFT CLUSTER.")
        except psycopg2.Error as e:
            print(e)
    
    def run_pipeline(self,
                     data_path: str=None, 
                     create_table_query: str=None) -> None:
        """
        Sequentially invokes extract(), transform()
        and load() methods.

        Args:
            data_path: Path to location with gpx files 
                       that will extracted.
            create_table_query: Query that will be used to create table.
        Returns:
            None 
        """
        self.extract(data_path)
        self.transform()
        self.load(create_table_query=create_table_query)
        print("PIPELINE EXECUTED SUCCESSFULLY!")        

    def execute(self,
                conn: connection,
                query_file: str,
                commit: bool=False) -> None:
        """
        Execute query provided in given sql file.

        Args:
            conn: Psycopg2 connection.
            query_file: Path to file containing query.
            commit: If True, the conn.commit() method will be
                    called after the query is executed. 
        Returns:
            None 
        """
        try:
            with conn.cursor() as cur:
                cur.execute(open(query_file, "r").read())
                if commit:
                    conn.commit()
        except psycopg2.Error as e:
            print(e)
    
    def _remove_anomalies(self, threshold: int=3) -> None:
        """
        Calculates Zscore for latitude and longitude and 
        then remove anomalies.

        Args:
            threshold: Used to determine anomalies.

        Returns:
            None
        """
        lat_zscore = np.abs(stats.zscore(self.transformed_data['latitude']))
        lon_zscore = np.abs(stats.zscore(self.transformed_data['longitude']))
        
        outliers_lat = np.where(lat_zscore > threshold)[0]
        outliers_lon = np.where(lon_zscore > threshold)[0]
        outliers = np.concatenate((outliers_lat, outliers_lon))
        self.transformed_data.drop(index=outliers,
                                   axis=0,
                                   inplace=True)
    
    def _get_start_and_destination_city(self,
                                        origin_latitude: str,
                                        origin_longitude: str,
                                        dest_latitude: str,
                                        dest_longitude: str) -> Tuple[str, str]:
        """
        Returns closest location based on origin and
        destination coordinates.

        Args:
            origin_latitude: Psycopg2 connection.
            origin_longitude: Path to file containing query.
            dest_latitude: Path to file containing query.
            dest_longitude: Path to file containing query.

        Returns:
            Origin location and destination location
        """
        origin_city = 'Unknown' 
        dest_city = 'Unknown'
        origin_location = self.geolocator.reverse(origin_latitude + "," + origin_longitude,
                                                 language='en').raw['address']
        dest_location = self.geolocator.reverse(dest_latitude + "," + dest_longitude,
                                                language='en').raw['address']
        if 'town' in origin_location:
            origin_city = origin_location['town']
        elif 'village' in origin_location:
            origin_city = origin_location['village']
        elif 'city' in origin_location:
            origin_city = origin_location['city']
        elif 'suburb' in origin_location:
            origin_city = origin_location['suburb']
        
        if 'town' in dest_location:
            dest_city = dest_location['town']
        elif 'village' in dest_location:
            dest_city = dest_location['village']
        elif 'city' in dest_location:
            dest_city = dest_location['city']
        elif 'suburb' in dest_location:
            dest_city = dest_location['suburb']
        
        return origin_city, dest_city
    
    def _prepare_conn_string(self, config: Dict[str, int]) -> str:
        """
        Creates connection string based ongiven configuraion.

        Args:
            config: Dict containing key/value pairs that will 
            be used as a connection parameters.
        Returns:
            Connection string. 
        """
        return f'postgresql://{config["user"]}:{config["password"]}' + \
               f'@{config["host"]}:{config["port"]}/{config["dbname"]}'
