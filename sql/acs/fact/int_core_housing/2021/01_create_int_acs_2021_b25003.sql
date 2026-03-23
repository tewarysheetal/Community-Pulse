DROP TABLE IF EXISTS public.int_acs_2021_b25003 CASCADE;

CREATE TABLE public.int_acs_2021_b25003 AS
SELECT
    CASE WHEN NULLIF(BTRIM("year"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("year")::numeric ELSE NULL END::int AS year,
    tract_geoid,
    CASE WHEN NULLIF(BTRIM("b25003_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25003_001e")::numeric ELSE NULL END AS occupied_units,
    CASE WHEN NULLIF(BTRIM("b25003_002e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25003_002e")::numeric ELSE NULL END AS owner_occupied_units,
    CASE WHEN NULLIF(BTRIM("b25003_003e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25003_003e")::numeric ELSE NULL END AS renter_occupied_units,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("b25003_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25003_001e")::numeric ELSE NULL END > 0 THEN ROUND(100.0 * CASE WHEN NULLIF(BTRIM("b25003_002e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25003_002e")::numeric ELSE NULL END / CASE WHEN NULLIF(BTRIM("b25003_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25003_001e")::numeric ELSE NULL END, 2)
        ELSE NULL
    END AS pct_owner_occupied,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("b25003_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25003_001e")::numeric ELSE NULL END > 0 THEN ROUND(100.0 * CASE WHEN NULLIF(BTRIM("b25003_003e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25003_003e")::numeric ELSE NULL END / CASE WHEN NULLIF(BTRIM("b25003_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25003_001e")::numeric ELSE NULL END, 2)
        ELSE NULL
    END AS pct_renter_occupied,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("b25003_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25003_001e")::numeric ELSE NULL END IS NOT NULL THEN 1
        ELSE 0
    END AS has_tenure_metric
FROM public.stg_acs_2021_b25003_raw;

CREATE INDEX idx_int_acs_2021_b25003_key
    ON public.int_acs_2021_b25003 (year, tract_geoid);
