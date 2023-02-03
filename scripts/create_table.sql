CREATE TABLE IF NOT EXISTS gpx_tracks (  
    id INT PRIMARY KEY IDENTITY(1, 1),  
    time DATETIME NOT NULL,  
    latitude FLOAT NOT NULL,  
    longitude FLOAT NOT NULL,  
    speed FLOAT,  
    course FLOAT,
    origin_city VARCHAR(255) NOT NULL,
    dest_city VARCHAR(255) NOT NULL,
    track_id  VARCHAR(255) NOT NULL 
);