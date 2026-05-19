-- install extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS hstore;

-- CREATE SCHEMA
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- CREATE TABLES
-- bronze tables
CREATE TABLE IF NOT EXISTS bronze.traffic_accidents_raw (
    raw_id BIGSERIAL PRIMARY KEY,
    payload JSONB,
    source TEXT,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.meteostat_raw (
    raw_id BIGSERIAL PRIMARY KEY,
    payload JSONB,
    source TEXT,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE bronze.osm_ways (
    osm_id BIGINT PRIMARY KEY,         -- Originální OSM ID
    geom GEOMETRY(Geometry, 4326), -- Surová geometrie ve WGS84
    tags HSTORE,                        -- Všechny OSM tagy (highway, maxspeed, atd.)
    nodes BIGINT[],                    -- Pole ID bodů (volitelné, pro integritu)
    ingested_at TIMESTAMP DEFAULT NOW()
);

-- silver tables
CREATE TABLE IF NOT EXISTS silver.traffic_accident_clean (
    -- identification
    accident_id SERIAL PRIMARY KEY,
    raw_id BIGINT,

    -- location
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,

    -- date
    date TIMESTAMP,

    -- accident type
    cause TEXT,
    collision_type TEXT,

    -- severity
    slightly_injured INT,
    severely_injured INT,
    fatalities INT,

    -- aftermath
    total_damage BIGINT
);

CREATE TABLE IF NOT EXISTS silver.weather_clean (
    -- location
    station_id TEXT NOT NULL,
    date DATE NOT NULL,
    time TIME NOT NULL,

    -- temperatures
    temp_c NUMERIC(5,2),
    precipitation_mm NUMERIC(6,2),
    snow INT,
    wind_dir INT,
    wind_speed NUMERIC(5,2),

    PRIMARY KEY (station_id, date, time)
);

CREATE TABLE IF NOT EXISTS silver.osm_roads_clean (
    road_id SERIAL PRIMARY KEY,
    name TEXT,
    road_type TEXT,
    is_intersection BOOLEAN,
    lanes SMALLINT,
    max_speed SMALLINT,
    oneway BOOLEAN,
    geom GEOMETRY(LineString, 4326)
);

-- gold tables
CREATE TABLE IF NOT EXISTS gold.dim_city (
    city_id SERIAL PRIMARY KEY,
    city_name TEXT UNIQUE,
    region TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    station_id TEXT
);

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_id SERIAL PRIMARY KEY,
    date DATE,
    year INT,
    month INT,
    day INT,
    day_of_week INT
);

CREATE TABLE IF NOT EXISTS gold.dim_road (
    road_id SERIAL PRIMARY KEY,
    road_type TEXT,
    is_intersection BOOLEAN,
    lanes SMALLINT,
    max_speed SMALLINT,
    oneway BOOLEAN
);

CREATE TABLE IF NOT EXISTS gold.dim_weather (
    weather_id SERIAL PRIMARY KEY,
    station_id TEXT NOT NULL,
    date DATE,
    temp_c NUMERIC(5,2),
    precipitation_mm NUMERIC(6,2),
    snow NUMERIC(6,2),
    wind_dir INT,
    wind_speed NUMERIC(5,2),

    weather_category TEXT,

    UNIQUE (station_id,date)
);

CREATE TABLE IF NOT EXISTS gold.fact_traffic_accidents (
    accident_id BIGINT PRIMARY KEY,

    -- foreign keys
    city_id INT REFERENCES gold.dim_city(city_id),
    date_id INT REFERENCES gold.dim_date(date_id),
    road_id INT REFERENCES gold.dim_road(road_id),
    weather_id INT REFERENCES gold.dim_weather(weather_id),

    -- measures
    total_damage BIGINT,

    -- optional attributes
    main_cause TEXT
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_dim_city_name ON gold.dim_city (city_name);
CREATE INDEX IF NOT EXISTS idx_dim_date ON gold.dim_date (date);
CREATE INDEX idx_osm_ways_tags ON bronze.osm_ways USING GIN (tags);
CREATE INDEX idx_osm_ways_geom ON bronze.osm_ways USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_osm_roads_geom 
ON silver.osm_roads_clean USING gist (geom);
