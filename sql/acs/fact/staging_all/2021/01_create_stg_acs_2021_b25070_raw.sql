DROP TABLE IF EXISTS stg_acs_2021_b25070_raw;

CREATE TABLE stg_acs_2021_b25070_raw (
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
    "b25070_001e" TEXT,
    "b25070_001m" TEXT,
    "b25070_002e" TEXT,
    "b25070_002m" TEXT,
    "b25070_003e" TEXT,
    "b25070_003m" TEXT,
    "b25070_004e" TEXT,
    "b25070_004m" TEXT,
    "b25070_005e" TEXT,
    "b25070_005m" TEXT,
    "b25070_006e" TEXT,
    "b25070_006m" TEXT,
    "b25070_007e" TEXT,
    "b25070_007m" TEXT,
    "b25070_008e" TEXT,
    "b25070_008m" TEXT,
    "b25070_009e" TEXT,
    "b25070_009m" TEXT,
    "b25070_010e" TEXT,
    "b25070_010m" TEXT,
    "b25070_011e" TEXT,
    "b25070_011m" TEXT,
    "unnamed_24" TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Manual load option
TRUNCATE TABLE stg_acs_2021_b25070_raw;

COPY stg_acs_2021_b25070_raw (
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
    "b25070_001e",
    "b25070_001m",
    "b25070_002e",
    "b25070_002m",
    "b25070_003e",
    "b25070_003m",
    "b25070_004e",
    "b25070_004m",
    "b25070_005e",
    "b25070_005m",
    "b25070_006e",
    "b25070_006m",
    "b25070_007e",
    "b25070_007m",
    "b25070_008e",
    "b25070_008m",
    "b25070_009e",
    "b25070_009m",
    "b25070_010e",
    "b25070_010m",
    "b25070_011e",
    "b25070_011m",
    "unnamed_24"
)
FROM 'D:/Projects/Community-Pulse/data/acs/processed/acs_tract/staging/2021/stg_acs_2021_b25070_raw.csv'
DELIMITER ','
CSV HEADER;

-- Validation
SELECT 'stg_acs_2021_b25070_raw' AS table_name, COUNT(*) AS row_count
FROM stg_acs_2021_b25070_raw;
