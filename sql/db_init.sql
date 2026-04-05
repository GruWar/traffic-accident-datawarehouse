-- install extensions
CREATE EXTENSION IF NOT EXISTS postgis;

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

CREATE TABLE IF NOT EXISTS bronze.osm_raw (
    raw_id BIGSERIAL PRIMARY KEY,
    payload JSONB,
    source TEXT,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- silver tables
CREATE TABLE IF NOT EXISTS silver.traffic_accident_clean (
    -- identification
    raw_id BIGINT PRIMARY KEY,
    accident_id BIGINT,

    -- location
    country TEXT DEFAULT 'CZ',
    city TEXT,
    district TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,

    -- date
    accident_time TIMESTAMP,

    -- accident type
    main_cause TEXT,
    injury_severity TEXT,

    -- conditions
    road_condition TEXT,
    weather_condition TEXT,
    visibility TEXT,

    -- vehicle
    vehicle_type TEXT,

    -- driver
    person_type TEXT,
    sex TEXT,
    age SMALLINT,
    alcohol BOOLEAN,

    -- aftermath
    total_damage BIGINT
);

CREATE TABLE IF NOT EXISTS silver.weather_clean (
    -- location
    station_id BIGINT NOT NULL,
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
    city_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS gold.dim_time (
    time_id SERIAL PRIMARY KEY,
    date DATE,
    month INT,
    day INT,
    day_of_week INT,
    hour SMALLINT
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

    temp_c NUMERIC(5,2),
    precipitation_mm NUMERIC(6,2),
    snow INT,
    wind_dir INT,
    wind_speed NUMERIC(5,2),

    weather_category TEXT
);

CREATE TABLE IF NOT EXISTS gold.fact_traffic_accidents (
    accident_id BIGINT PRIMARY KEY,

    -- foreign keys
    city_id INT,
    time_id INT,
    road_id INT,
    weather_id INT,

    -- measures
    total_damage BIGINT,

    -- optional attributes
    main_cause TEXT
);

CREATE TABLE IF NOT EXISTS gold.traffic_accident_person (
    accident_person_id SERIAL PRIMARY KEY,
    accident_id BIGINT,
    sex TEXT,
    age SMALLINT,
    alcohol BOOLEAN,
    injury_severity TEXT,
    FOREIGN KEY (accident_id) REFERENCES gold.fact_traffic_accidents(accident_id)
);
