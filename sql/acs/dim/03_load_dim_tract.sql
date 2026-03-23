TRUNCATE TABLE dim_tract;

COPY dim_tract (
    tract_geoid,
    geo_id,
    statefp,
    countyfp,
    tractce,
    tract_number,
    tract_name_canonical,
    tract_name_latest,
    county_name,
    state_name,
    first_year_seen,
    last_year_seen,
    year_count,
    is_stable_all_4_years
)
FROM 'D:/Projects/Community-Pulse/data/acs/processed/acs_tract/dimensions/dim_tract.csv'
DELIMITER ','
CSV HEADER;