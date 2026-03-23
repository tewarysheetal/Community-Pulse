DROP TABLE IF EXISTS public.int_acs_2019_b19013 CASCADE;

CREATE TABLE public.int_acs_2019_b19013 AS
SELECT
    CASE WHEN NULLIF(BTRIM("year"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("year")::numeric ELSE NULL END::int AS year,
    tract_geoid,
    CASE WHEN NULLIF(BTRIM("b19013_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b19013_001e")::numeric ELSE NULL END AS median_household_income,
    CASE WHEN NULLIF(BTRIM("b19013_001m"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b19013_001m")::numeric ELSE NULL END AS median_household_income_moe,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("b19013_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b19013_001e")::numeric ELSE NULL END IS NOT NULL THEN 1
        ELSE 0
    END AS has_income_metric
FROM public.stg_acs_2019_b19013_raw;

CREATE INDEX idx_int_acs_2019_b19013_key
    ON public.int_acs_2019_b19013 (year, tract_geoid);
