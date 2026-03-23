DROP VIEW IF EXISTS public.vw_acs_tract_metric_long_stable_4yr;
DROP VIEW IF EXISTS public.vw_acs_tract_metric_long_2023;
DROP VIEW IF EXISTS public.vw_acs_tract_metric_long_all_years;

DROP TABLE IF EXISTS public.fact_acs_tract_metric_long;

CREATE TABLE public.fact_acs_tract_metric_long (
    year            INTEGER NOT NULL,
    tract_geoid     VARCHAR(11) NOT NULL,
    source_table    VARCHAR(20) NOT NULL,
    metric_group    VARCHAR(50) NOT NULL,
    metric_name     VARCHAR(255) NOT NULL,
    metric_kind     VARCHAR(20) NOT NULL,
    metric_value    NUMERIC,
    geo_id          VARCHAR(20),
    name            TEXT,
    statefp         VARCHAR(2),
    countyfp        VARCHAR(3),
    tractce         VARCHAR(6),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_acs_tract_metric_long_key
    ON public.fact_acs_tract_metric_long (year, tract_geoid);

CREATE INDEX idx_fact_acs_tract_metric_long_metric
    ON public.fact_acs_tract_metric_long (source_table, metric_name);

CREATE INDEX idx_fact_acs_tract_metric_long_group
    ON public.fact_acs_tract_metric_long (metric_group, metric_kind);

CREATE VIEW public.vw_acs_tract_metric_long_all_years AS
SELECT
    f.*,
    d.tract_number,
    d.tract_name_canonical,
    d.tract_name_latest,
    d.county_name,
    d.state_name,
    d.year_count,
    d.is_stable_all_4_years,
    y.year_label,
    y.acs_dataset,
    y.acs_period
FROM public.fact_acs_tract_metric_long f
LEFT JOIN public.dim_tract d
  ON d.tract_geoid = f.tract_geoid
LEFT JOIN public.dim_year y
  ON y.year = f.year;

CREATE VIEW public.vw_acs_tract_metric_long_2023 AS
SELECT *
FROM public.vw_acs_tract_metric_long_all_years
WHERE year = 2023;

CREATE VIEW public.vw_acs_tract_metric_long_stable_4yr AS
SELECT *
FROM public.vw_acs_tract_metric_long_all_years
WHERE is_stable_all_4_years = 1;
