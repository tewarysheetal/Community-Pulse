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
