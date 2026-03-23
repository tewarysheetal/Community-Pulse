DROP TABLE IF EXISTS stg_acs_2019_b25003_raw;

CREATE TABLE stg_acs_2019_b25003_raw (
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
    "b25003_001e" TEXT,
    "b25003_001m" TEXT,
    "b25003_002e" TEXT,
    "b25003_002m" TEXT,
    "b25003_003e" TEXT,
    "b25003_003m" TEXT,
    "unnamed_8" TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Manual load option
TRUNCATE TABLE stg_acs_2019_b25003_raw;

COPY stg_acs_2019_b25003_raw (
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
    "b25003_001e",
    "b25003_001m",
    "b25003_002e",
    "b25003_002m",
    "b25003_003e",
    "b25003_003m",
    "unnamed_8"
)
FROM 'D:/Projects/Community-Pulse/data/acs/processed/acs_tract/staging/2019/stg_acs_2019_b25003_raw.csv'
DELIMITER ','
CSV HEADER;

-- Validation
SELECT 'stg_acs_2019_b25003_raw' AS table_name, COUNT(*) AS row_count
FROM stg_acs_2019_b25003_raw;
