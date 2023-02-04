# GPX-TRACKS-ETL

# Setup

Before running the code there is a need to create a .env file
in a etlgpx directory containing following variables:

(There is an option to just create following variables as environment variables, but
it is not supported by Dockerfile)

- REDSHIFT_DB_NAME: Redshift database name
- REDSHIFT_HOST: Redshift host url
- REDSHIFT_PORT: Port that will be used
- REDSHIFT_USER: Redshift username
- REDSHIFT_PASSWORD: Redshift password for given user
- REDSHIFT_TABLE: Name of the redshift table
- REDSHIFT_SCHEMA: Name of the redshift schema
- CREATE_TABLE: path to sql file containing code that create a table
- DATA_PATH: path to the data that will be used to run pipeline

# Running the code

- From within project directory

  1. Install
     pip3 install -e . -r requirements.txt
  2. Run code

     - python3 etlgpx/main.py

     or

     - python3 etlhpx/cli.py PARAMS

- As pip package

  1. Install
     pip3 install etlgpx
  2. Run code
     - etlgpx PARAMS

Supported PARAMS:

     - operation:
       Type of an operation that will be performed. Possible values: extract | transform | load | execute

     - data:
       Path to the data in gpx format that will be extracted.
     - transform_data:
       Data that will be transformed.
       Supported format: List[GPXTrack].

     - create_table:
       Path to a sql script that will be
       used to create a table that will
       store prepared data

# Architecture

- Processor

  I decided to create a Processor class that is responsible for the ETL process. It consists of separated methods for
  extracting, transforming and loading data. Additionally there is a run_pipeline() method that makes use of
  all these three methods and runs the whole ETL process.

  Processor also implements a method responsible for cleaning data by removing anomalies calculated based on
  given latitude and longitude.

  There is also a method that returns the closest city based on given coordinates.

  Each point is checked for whether it is on the land. If so, it is removed from the data set.

  Data coming from gpx files is transformed into pandas Dataframes and then loaded into Redshift.

- Data model

  Table used to store data looks as follows:
  id INT PRIMARY KEY IDENTITY(1, 1),  
  time DATETIME NOT NULL,  
  latitude FLOAT NOT NULL,  
  longitude FLOAT NOT NULL,  
  speed FLOAT,  
  course FLOAT,
  origin_city VARCHAR(255) NOT NULL,
  dest_city VARCHAR(255) NOT NULL,
  track_id VARCHAR(255) NOT NULL

  To the information stored in gpx files I added origin_city, dest_city and track_id. At the beginning I wanted to store each track as a one row in a database, but I thought that it might not be the most efficient way, so I decided to create a unique track_id and assign it to every point with respect to the track that this point is a part of. Thanks to that I got flatten structure of data and also there is an option to use track_id to group points into respecting tracks.

  track_id is created uniquely with uuid.uuid4().hex

- Scripts
  I created a scripts directory to store sql scripts.

  insights1.sql is a query that answers the following question:
  What percentage of tracks exceeded 10 knots (nautical miles per hour)?

  insights2.sql is a query that answers the following question:
  Breakdown tracks to and from Cowes to find the origin/destination (nearest town)
  and what percentage started/ended in each

- Pipeline steps:
  1. Data is extracted from gpx files stored in a given directory
  2. Extracted is cleansed and transformed into the previously mentioned data format.
  3. Transformed data is loaded into Redshift based on given configuration.
