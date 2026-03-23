-- int_acs_2019_b19013
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

-- int_acs_2019_b25003
DROP TABLE IF EXISTS public.int_acs_2019_b25003 CASCADE;

CREATE TABLE public.int_acs_2019_b25003 AS
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
FROM public.stg_acs_2019_b25003_raw;

CREATE INDEX idx_int_acs_2019_b25003_key
    ON public.int_acs_2019_b25003 (year, tract_geoid);

-- int_acs_2019_b25070
DROP TABLE IF EXISTS public.int_acs_2019_b25070 CASCADE;

CREATE TABLE public.int_acs_2019_b25070 AS
SELECT
    CASE WHEN NULLIF(BTRIM("year"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("year")::numeric ELSE NULL END::int AS year,
    tract_geoid,
    CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END AS renter_hh_rent_burden_base,
    CASE WHEN NULLIF(BTRIM("b25070_007e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_007e")::numeric ELSE NULL END AS rent_30_34,
    CASE WHEN NULLIF(BTRIM("b25070_008e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_008e")::numeric ELSE NULL END AS rent_35_39,
    CASE WHEN NULLIF(BTRIM("b25070_009e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_009e")::numeric ELSE NULL END AS rent_40_49,
    CASE WHEN NULLIF(BTRIM("b25070_010e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_010e")::numeric ELSE NULL END AS rent_50_plus,
    CASE WHEN NULLIF(BTRIM("b25070_011e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_011e")::numeric ELSE NULL END AS rent_not_computed,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END > 0
        THEN ROUND(100.0 * (COALESCE(CASE WHEN NULLIF(BTRIM("b25070_007e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_007e")::numeric ELSE NULL END, 0) + COALESCE(CASE WHEN NULLIF(BTRIM("b25070_008e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_008e")::numeric ELSE NULL END, 0) + COALESCE(CASE WHEN NULLIF(BTRIM("b25070_009e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_009e")::numeric ELSE NULL END, 0) + COALESCE(CASE WHEN NULLIF(BTRIM("b25070_010e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_010e")::numeric ELSE NULL END, 0)) / CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END, 2)
        ELSE NULL
    END AS pct_rent_burden_30_plus,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END > 0
        THEN ROUND(100.0 * COALESCE(CASE WHEN NULLIF(BTRIM("b25070_010e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_010e")::numeric ELSE NULL END, 0) / CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END, 2)
        ELSE NULL
    END AS pct_rent_burden_50_plus,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END > 0
        THEN ROUND(100.0 * COALESCE(CASE WHEN NULLIF(BTRIM("b25070_011e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_011e")::numeric ELSE NULL END, 0) / CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END, 2)
        ELSE NULL
    END AS pct_rent_not_computed,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END IS NOT NULL THEN 1
        ELSE 0
    END AS has_rent_burden_metric
FROM public.stg_acs_2019_b25070_raw;

CREATE INDEX idx_int_acs_2019_b25070_key
    ON public.int_acs_2019_b25070 (year, tract_geoid);

-- int_acs_2019_dp04
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

-- int_acs_2021_b19013
DROP TABLE IF EXISTS public.int_acs_2021_b19013 CASCADE;

CREATE TABLE public.int_acs_2021_b19013 AS
SELECT
    CASE WHEN NULLIF(BTRIM("year"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("year")::numeric ELSE NULL END::int AS year,
    tract_geoid,
    CASE WHEN NULLIF(BTRIM("b19013_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b19013_001e")::numeric ELSE NULL END AS median_household_income,
    CASE WHEN NULLIF(BTRIM("b19013_001m"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b19013_001m")::numeric ELSE NULL END AS median_household_income_moe,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("b19013_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b19013_001e")::numeric ELSE NULL END IS NOT NULL THEN 1
        ELSE 0
    END AS has_income_metric
FROM public.stg_acs_2021_b19013_raw;

CREATE INDEX idx_int_acs_2021_b19013_key
    ON public.int_acs_2021_b19013 (year, tract_geoid);

-- int_acs_2021_b25003
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

-- int_acs_2021_b25070
DROP TABLE IF EXISTS public.int_acs_2021_b25070 CASCADE;

CREATE TABLE public.int_acs_2021_b25070 AS
SELECT
    CASE WHEN NULLIF(BTRIM("year"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("year")::numeric ELSE NULL END::int AS year,
    tract_geoid,
    CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END AS renter_hh_rent_burden_base,
    CASE WHEN NULLIF(BTRIM("b25070_007e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_007e")::numeric ELSE NULL END AS rent_30_34,
    CASE WHEN NULLIF(BTRIM("b25070_008e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_008e")::numeric ELSE NULL END AS rent_35_39,
    CASE WHEN NULLIF(BTRIM("b25070_009e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_009e")::numeric ELSE NULL END AS rent_40_49,
    CASE WHEN NULLIF(BTRIM("b25070_010e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_010e")::numeric ELSE NULL END AS rent_50_plus,
    CASE WHEN NULLIF(BTRIM("b25070_011e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_011e")::numeric ELSE NULL END AS rent_not_computed,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END > 0
        THEN ROUND(100.0 * (COALESCE(CASE WHEN NULLIF(BTRIM("b25070_007e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_007e")::numeric ELSE NULL END, 0) + COALESCE(CASE WHEN NULLIF(BTRIM("b25070_008e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_008e")::numeric ELSE NULL END, 0) + COALESCE(CASE WHEN NULLIF(BTRIM("b25070_009e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_009e")::numeric ELSE NULL END, 0) + COALESCE(CASE WHEN NULLIF(BTRIM("b25070_010e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_010e")::numeric ELSE NULL END, 0)) / CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END, 2)
        ELSE NULL
    END AS pct_rent_burden_30_plus,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END > 0
        THEN ROUND(100.0 * COALESCE(CASE WHEN NULLIF(BTRIM("b25070_010e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_010e")::numeric ELSE NULL END, 0) / CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END, 2)
        ELSE NULL
    END AS pct_rent_burden_50_plus,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END > 0
        THEN ROUND(100.0 * COALESCE(CASE WHEN NULLIF(BTRIM("b25070_011e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_011e")::numeric ELSE NULL END, 0) / CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END, 2)
        ELSE NULL
    END AS pct_rent_not_computed,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END IS NOT NULL THEN 1
        ELSE 0
    END AS has_rent_burden_metric
FROM public.stg_acs_2021_b25070_raw;

CREATE INDEX idx_int_acs_2021_b25070_key
    ON public.int_acs_2021_b25070 (year, tract_geoid);

-- int_acs_2021_dp04
DROP TABLE IF EXISTS public.int_acs_2021_dp04 CASCADE;

CREATE TABLE public.int_acs_2021_dp04 AS
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
FROM public.stg_acs_2021_dp04_raw;

CREATE INDEX idx_int_acs_2021_dp04_key
    ON public.int_acs_2021_dp04 (year, tract_geoid);

-- int_acs_2022_b19013
DROP TABLE IF EXISTS public.int_acs_2022_b19013 CASCADE;

CREATE TABLE public.int_acs_2022_b19013 AS
SELECT
    CASE WHEN NULLIF(BTRIM("year"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("year")::numeric ELSE NULL END::int AS year,
    tract_geoid,
    CASE WHEN NULLIF(BTRIM("b19013_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b19013_001e")::numeric ELSE NULL END AS median_household_income,
    CASE WHEN NULLIF(BTRIM("b19013_001m"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b19013_001m")::numeric ELSE NULL END AS median_household_income_moe,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("b19013_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b19013_001e")::numeric ELSE NULL END IS NOT NULL THEN 1
        ELSE 0
    END AS has_income_metric
FROM public.stg_acs_2022_b19013_raw;

CREATE INDEX idx_int_acs_2022_b19013_key
    ON public.int_acs_2022_b19013 (year, tract_geoid);

-- int_acs_2022_b25003
DROP TABLE IF EXISTS public.int_acs_2022_b25003 CASCADE;

CREATE TABLE public.int_acs_2022_b25003 AS
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
FROM public.stg_acs_2022_b25003_raw;

CREATE INDEX idx_int_acs_2022_b25003_key
    ON public.int_acs_2022_b25003 (year, tract_geoid);

-- int_acs_2022_b25070
DROP TABLE IF EXISTS public.int_acs_2022_b25070 CASCADE;

CREATE TABLE public.int_acs_2022_b25070 AS
SELECT
    CASE WHEN NULLIF(BTRIM("year"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("year")::numeric ELSE NULL END::int AS year,
    tract_geoid,
    CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END AS renter_hh_rent_burden_base,
    CASE WHEN NULLIF(BTRIM("b25070_007e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_007e")::numeric ELSE NULL END AS rent_30_34,
    CASE WHEN NULLIF(BTRIM("b25070_008e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_008e")::numeric ELSE NULL END AS rent_35_39,
    CASE WHEN NULLIF(BTRIM("b25070_009e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_009e")::numeric ELSE NULL END AS rent_40_49,
    CASE WHEN NULLIF(BTRIM("b25070_010e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_010e")::numeric ELSE NULL END AS rent_50_plus,
    CASE WHEN NULLIF(BTRIM("b25070_011e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_011e")::numeric ELSE NULL END AS rent_not_computed,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END > 0
        THEN ROUND(100.0 * (COALESCE(CASE WHEN NULLIF(BTRIM("b25070_007e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_007e")::numeric ELSE NULL END, 0) + COALESCE(CASE WHEN NULLIF(BTRIM("b25070_008e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_008e")::numeric ELSE NULL END, 0) + COALESCE(CASE WHEN NULLIF(BTRIM("b25070_009e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_009e")::numeric ELSE NULL END, 0) + COALESCE(CASE WHEN NULLIF(BTRIM("b25070_010e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_010e")::numeric ELSE NULL END, 0)) / CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END, 2)
        ELSE NULL
    END AS pct_rent_burden_30_plus,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END > 0
        THEN ROUND(100.0 * COALESCE(CASE WHEN NULLIF(BTRIM("b25070_010e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_010e")::numeric ELSE NULL END, 0) / CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END, 2)
        ELSE NULL
    END AS pct_rent_burden_50_plus,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END > 0
        THEN ROUND(100.0 * COALESCE(CASE WHEN NULLIF(BTRIM("b25070_011e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_011e")::numeric ELSE NULL END, 0) / CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END, 2)
        ELSE NULL
    END AS pct_rent_not_computed,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END IS NOT NULL THEN 1
        ELSE 0
    END AS has_rent_burden_metric
FROM public.stg_acs_2022_b25070_raw;

CREATE INDEX idx_int_acs_2022_b25070_key
    ON public.int_acs_2022_b25070 (year, tract_geoid);

-- int_acs_2022_dp04
DROP TABLE IF EXISTS public.int_acs_2022_dp04 CASCADE;

CREATE TABLE public.int_acs_2022_dp04 AS
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
FROM public.stg_acs_2022_dp04_raw;

CREATE INDEX idx_int_acs_2022_dp04_key
    ON public.int_acs_2022_dp04 (year, tract_geoid);

-- int_acs_2023_b19013
DROP TABLE IF EXISTS public.int_acs_2023_b19013 CASCADE;

CREATE TABLE public.int_acs_2023_b19013 AS
SELECT
    CASE WHEN NULLIF(BTRIM("year"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("year")::numeric ELSE NULL END::int AS year,
    tract_geoid,
    CASE WHEN NULLIF(BTRIM("b19013_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b19013_001e")::numeric ELSE NULL END AS median_household_income,
    CASE WHEN NULLIF(BTRIM("b19013_001m"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b19013_001m")::numeric ELSE NULL END AS median_household_income_moe,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("b19013_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b19013_001e")::numeric ELSE NULL END IS NOT NULL THEN 1
        ELSE 0
    END AS has_income_metric
FROM public.stg_acs_2023_b19013_raw;

CREATE INDEX idx_int_acs_2023_b19013_key
    ON public.int_acs_2023_b19013 (year, tract_geoid);

-- int_acs_2023_b25003
DROP TABLE IF EXISTS public.int_acs_2023_b25003 CASCADE;

CREATE TABLE public.int_acs_2023_b25003 AS
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
FROM public.stg_acs_2023_b25003_raw;

CREATE INDEX idx_int_acs_2023_b25003_key
    ON public.int_acs_2023_b25003 (year, tract_geoid);

-- int_acs_2023_b25070
DROP TABLE IF EXISTS public.int_acs_2023_b25070 CASCADE;

CREATE TABLE public.int_acs_2023_b25070 AS
SELECT
    CASE WHEN NULLIF(BTRIM("year"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("year")::numeric ELSE NULL END::int AS year,
    tract_geoid,
    CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END AS renter_hh_rent_burden_base,
    CASE WHEN NULLIF(BTRIM("b25070_007e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_007e")::numeric ELSE NULL END AS rent_30_34,
    CASE WHEN NULLIF(BTRIM("b25070_008e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_008e")::numeric ELSE NULL END AS rent_35_39,
    CASE WHEN NULLIF(BTRIM("b25070_009e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_009e")::numeric ELSE NULL END AS rent_40_49,
    CASE WHEN NULLIF(BTRIM("b25070_010e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_010e")::numeric ELSE NULL END AS rent_50_plus,
    CASE WHEN NULLIF(BTRIM("b25070_011e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_011e")::numeric ELSE NULL END AS rent_not_computed,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END > 0
        THEN ROUND(100.0 * (COALESCE(CASE WHEN NULLIF(BTRIM("b25070_007e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_007e")::numeric ELSE NULL END, 0) + COALESCE(CASE WHEN NULLIF(BTRIM("b25070_008e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_008e")::numeric ELSE NULL END, 0) + COALESCE(CASE WHEN NULLIF(BTRIM("b25070_009e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_009e")::numeric ELSE NULL END, 0) + COALESCE(CASE WHEN NULLIF(BTRIM("b25070_010e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_010e")::numeric ELSE NULL END, 0)) / CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END, 2)
        ELSE NULL
    END AS pct_rent_burden_30_plus,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END > 0
        THEN ROUND(100.0 * COALESCE(CASE WHEN NULLIF(BTRIM("b25070_010e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_010e")::numeric ELSE NULL END, 0) / CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END, 2)
        ELSE NULL
    END AS pct_rent_burden_50_plus,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END > 0
        THEN ROUND(100.0 * COALESCE(CASE WHEN NULLIF(BTRIM("b25070_011e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_011e")::numeric ELSE NULL END, 0) / CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END, 2)
        ELSE NULL
    END AS pct_rent_not_computed,
    CASE
        WHEN CASE WHEN NULLIF(BTRIM("b25070_001e"), '') ~ '^-?\d+(\.\d+)?$' THEN BTRIM("b25070_001e")::numeric ELSE NULL END IS NOT NULL THEN 1
        ELSE 0
    END AS has_rent_burden_metric
FROM public.stg_acs_2023_b25070_raw;

CREATE INDEX idx_int_acs_2023_b25070_key
    ON public.int_acs_2023_b25070 (year, tract_geoid);

-- int_acs_2023_dp04
DROP TABLE IF EXISTS public.int_acs_2023_dp04 CASCADE;

CREATE TABLE public.int_acs_2023_dp04 AS
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
FROM public.stg_acs_2023_dp04_raw;

CREATE INDEX idx_int_acs_2023_dp04_key
    ON public.int_acs_2023_dp04 (year, tract_geoid);

-- vw_int_acs_b19013_all_years
DROP VIEW IF EXISTS public.vw_int_acs_b19013_all_years;

CREATE VIEW public.vw_int_acs_b19013_all_years AS
SELECT * FROM public.int_acs_2019_b19013
UNION ALL
SELECT * FROM public.int_acs_2021_b19013
UNION ALL
SELECT * FROM public.int_acs_2022_b19013
UNION ALL
SELECT * FROM public.int_acs_2023_b19013;

-- vw_int_acs_b25003_all_years
DROP VIEW IF EXISTS public.vw_int_acs_b25003_all_years;

CREATE VIEW public.vw_int_acs_b25003_all_years AS
SELECT * FROM public.int_acs_2019_b25003
UNION ALL
SELECT * FROM public.int_acs_2021_b25003
UNION ALL
SELECT * FROM public.int_acs_2022_b25003
UNION ALL
SELECT * FROM public.int_acs_2023_b25003;

-- vw_int_acs_b25070_all_years
DROP VIEW IF EXISTS public.vw_int_acs_b25070_all_years;

CREATE VIEW public.vw_int_acs_b25070_all_years AS
SELECT * FROM public.int_acs_2019_b25070
UNION ALL
SELECT * FROM public.int_acs_2021_b25070
UNION ALL
SELECT * FROM public.int_acs_2022_b25070
UNION ALL
SELECT * FROM public.int_acs_2023_b25070;

-- vw_int_acs_dp04_all_years
DROP VIEW IF EXISTS public.vw_int_acs_dp04_all_years;

CREATE VIEW public.vw_int_acs_dp04_all_years AS
SELECT * FROM public.int_acs_2019_dp04
UNION ALL
SELECT * FROM public.int_acs_2021_dp04
UNION ALL
SELECT * FROM public.int_acs_2022_dp04
UNION ALL
SELECT * FROM public.int_acs_2023_dp04;

