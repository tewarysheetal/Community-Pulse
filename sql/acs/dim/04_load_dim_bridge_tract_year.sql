TRUNCATE TABLE bridge_tract_year;

COPY bridge_tract_year (
    year,
    tract_geoid,
    geo_id,
    statefp,
    countyfp,
    tractce,
    tract_number,
    tract_name_raw
)
FROM 'D:/Projects/Community-Pulse/data/acs/processed/acs_tract/dimensions/bridge_tract_year.csv'
DELIMITER ','
CSV HEADER;