DROP TABLE IF EXISTS dim_year;

CREATE TABLE dim_year (
    year                    INTEGER PRIMARY KEY,
    year_label              VARCHAR(20) NOT NULL,
    acs_dataset             VARCHAR(50) NOT NULL,
    acs_period              VARCHAR(20) NOT NULL,
    is_experimental         INTEGER NOT NULL DEFAULT 0,
    is_in_pipeline          INTEGER NOT NULL DEFAULT 1,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO dim_year (
    year,
    year_label,
    acs_dataset,
    acs_period,
    is_experimental,
    is_in_pipeline
)
VALUES
    (2019, 'ACS 2019 5-Year', 'ACS5', '5-Year', 0, 1),
    (2021, 'ACS 2021 5-Year', 'ACS5', '5-Year', 0, 1),
    (2022, 'ACS 2022 5-Year', 'ACS5', '5-Year', 0, 1),
    (2023, 'ACS 2023 5-Year', 'ACS5', '5-Year', 0, 1);