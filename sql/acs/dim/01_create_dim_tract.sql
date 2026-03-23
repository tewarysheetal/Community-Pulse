DROP TABLE IF EXISTS dim_tract;

CREATE TABLE dim_tract (
    tract_geoid             VARCHAR(11) PRIMARY KEY,
    geo_id                  VARCHAR(20) NOT NULL,
    statefp                 VARCHAR(2) NOT NULL,
    countyfp                VARCHAR(3) NOT NULL,
    tractce                 VARCHAR(6) NOT NULL,
    tract_number            VARCHAR(20),
    tract_name_canonical    VARCHAR(200) NOT NULL,
    tract_name_latest       VARCHAR(200),
    county_name             VARCHAR(100) NOT NULL,
    state_name              VARCHAR(100) NOT NULL,
    first_year_seen         INTEGER NOT NULL,
    last_year_seen          INTEGER NOT NULL,
    year_count              INTEGER NOT NULL,
    is_stable_all_4_years   INTEGER NOT NULL,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_tract_state_county
    ON dim_tract (statefp, countyfp);

CREATE INDEX idx_dim_tract_tractce
    ON dim_tract (tractce);

CREATE INDEX idx_dim_tract_stability
    ON dim_tract (is_stable_all_4_years, year_count);