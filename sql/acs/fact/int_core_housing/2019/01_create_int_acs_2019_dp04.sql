DROP TABLE IF EXISTS public.int_acs_2019_dp04 CASCADE;

CREATE TABLE public.int_acs_2019_dp04 AS
SELECT
    CASE WHEN NULLIF(BTRIM("year"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("year")::numeric ELSE NULL END::int AS year,
    tract_geoid,
    CASE WHEN NULLIF(BTRIM("dp04_0001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("dp04_0001e")::numeric ELSE NULL END AS housing_units_total,
    CASE WHEN NULLIF(BTRIM("dp04_0002e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("dp04_0002e")::numeric ELSE NULL END AS occupied_housing_units_dp04,
    CASE WHEN NULLIF(BTRIM("dp04_0003e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("dp04_0003e")::numeric ELSE NULL END AS vacant_housing_units_dp04,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("dp04_0001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("dp04_0001e")::numeric ELSE NULL END > 0
        THEN ROUND(100.0 * CASE WHEN NULLIF(BTRIM("dp04_0002e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("dp04_0002e")::numeric ELSE NULL END / CASE WHEN NULLIF(BTRIM("dp04_0001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("dp04_0001e")::numeric ELSE NULL END, 2)
        ELSE NULL
    END AS pct_occupied_housing_units,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("dp04_0001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("dp04_0001e")::numeric ELSE NULL END > 0
        THEN ROUND(100.0 * CASE WHEN NULLIF(BTRIM("dp04_0003e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("dp04_0003e")::numeric ELSE NULL END / CASE WHEN NULLIF(BTRIM("dp04_0001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("dp04_0001e")::numeric ELSE NULL END, 2)
        ELSE NULL
    END AS pct_vacant_housing_units,
    CASE WHEN NULLIF(BTRIM("dp04_0004e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("dp04_0004e")::numeric ELSE NULL END AS for_rent_units,
    CASE WHEN NULLIF(BTRIM("dp04_0005e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("dp04_0005e")::numeric ELSE NULL END AS rented_not_occupied_units,
    CASE WHEN NULLIF(BTRIM("dp04_0006e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("dp04_0006e")::numeric ELSE NULL END AS for_sale_only_units,
    CASE WHEN NULLIF(BTRIM("dp04_0007e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("dp04_0007e")::numeric ELSE NULL END AS sold_not_occupied_units,
    CASE WHEN NULLIF(BTRIM("dp04_0008e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("dp04_0008e")::numeric ELSE NULL END AS seasonal_recreational_units,
    CASE WHEN NULLIF(BTRIM("dp04_0009e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("dp04_0009e")::numeric ELSE NULL END AS migrant_worker_units,
    CASE WHEN NULLIF(BTRIM("dp04_0010e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("dp04_0010e")::numeric ELSE NULL END AS other_vacant_units,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("dp04_0001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("dp04_0001e")::numeric ELSE NULL END IS NOT NULL THEN 1
        ELSE 0
    END AS has_housing_profile_metric
FROM public.stg_acs_2019_dp04_raw;

CREATE INDEX idx_int_acs_2019_dp04_key
    ON public.int_acs_2019_dp04 (year, tract_geoid);
