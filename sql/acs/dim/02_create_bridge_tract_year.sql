DROP TABLE IF EXISTS bridge_tract_year;

CREATE TABLE bridge_tract_year (
    year            INTEGER NOT NULL,
    tract_geoid     VARCHAR(11) NOT NULL,
    geo_id          VARCHAR(20) NOT NULL,
    statefp         VARCHAR(2) NOT NULL,
    countyfp        VARCHAR(3) NOT NULL,
    tractce         VARCHAR(6) NOT NULL,
    tract_number    VARCHAR(20),
    tract_name_raw  VARCHAR(200),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (year, tract_geoid)
);

CREATE INDEX idx_bridge_tract_year_tract
    ON bridge_tract_year (tract_geoid);

CREATE INDEX idx_bridge_tract_year_year
    ON bridge_tract_year (year);