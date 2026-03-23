DROP TABLE IF EXISTS stg_acs_2021_b19013_raw;

CREATE TABLE stg_acs_2021_b19013_raw (
    "year" TEXT,
    "table_code" TEXT,
    "table_family" TEXT,
    "source_type" TEXT,
    "geo_id" TEXT,
    "name" TEXT,
    "tract_geoid" TEXT,
    "statefp" TEXT,
    "countyfp" TEXT,
    "tractce" TEXT,
    "geo_id_2" TEXT,
    "name_2" TEXT,
    "b19013_001e" TEXT,
    "b19013_001m" TEXT,
    "unnamed_4" TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Manual load option
TRUNCATE TABLE stg_acs_2021_b19013_raw;

COPY stg_acs_2021_b19013_raw (
    "year",
    "table_code",
    "table_family",
    "source_type",
    "geo_id",
    "name",
    "tract_geoid",
    "statefp",
    "countyfp",
    "tractce",
    "geo_id_2",
    "name_2",
    "b19013_001e",
    "b19013_001m",
    "unnamed_4"
)
FROM 'D:/Projects/Community-Pulse/data/acs/processed/acs_tract/staging/2021/stg_acs_2021_b19013_raw.csv'
DELIMITER ','
CSV HEADER;

-- Validation
SELECT 'stg_acs_2021_b19013_raw' AS table_name, COUNT(*) AS row_count
FROM stg_acs_2021_b19013_raw;
