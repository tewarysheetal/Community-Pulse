DROP VIEW IF EXISTS public.vw_acs_tract_profile_cluster_input;
DROP VIEW IF EXISTS public.vw_acs_tract_profile_stable_4yr;
DROP VIEW IF EXISTS public.vw_acs_tract_profile_2023;
DROP VIEW IF EXISTS public.vw_acs_tract_profile_all_years;

DROP TABLE IF EXISTS public.fact_acs_tract_profile;

CREATE TABLE public.fact_acs_tract_profile AS
WITH base AS (
    SELECT
        b.year,
        b.tract_geoid,
        d.geo_id,
        d.statefp,
        d.countyfp,
        d.tractce,
        d.tract_number,
        d.tract_name_canonical,
        d.tract_name_latest,
        d.county_name,
        d.state_name,
        d.first_year_seen,
        d.last_year_seen,
        d.year_count,
        d.is_stable_all_4_years,
        y.year_label,
        y.acs_dataset,
        y.acs_period
    FROM public.bridge_tract_year b
    JOIN public.dim_tract d
      ON d.tract_geoid = b.tract_geoid
    JOIN public.dim_year y
      ON y.year = b.year
)
SELECT
    base.year,
    base.tract_geoid,
    base.geo_id,
    base.statefp,
    base.countyfp,
    base.tractce,
    base.tract_number,
    base.tract_name_canonical,
    base.tract_name_latest,
    base.county_name,
    base.state_name,
    base.first_year_seen,
    base.last_year_seen,
    base.year_count,
    base.is_stable_all_4_years,
    base.year_label,
    base.acs_dataset,
    base.acs_period,

    -- Curated wide metrics for EDA / clustering
    b19013.median_household_income,
    b19013.median_household_income_moe,

    b25003.occupied_units,
    b25003.owner_occupied_units,
    b25003.renter_occupied_units,
    b25003.pct_owner_occupied,
    b25003.pct_renter_occupied,

    b25070.renter_hh_rent_burden_base,
    b25070.rent_30_34,
    b25070.rent_35_39,
    b25070.rent_40_49,
    b25070.rent_50_plus,
    b25070.rent_not_computed,
    b25070.pct_rent_burden_30_plus,
    b25070.pct_rent_burden_50_plus,
    b25070.pct_rent_not_computed,

    dp04.housing_units_total,
    dp04.occupied_housing_units_dp04,
    dp04.vacant_housing_units_dp04,
    dp04.pct_occupied_housing_units,
    dp04.pct_vacant_housing_units,
    dp04.for_rent_units,
    dp04.rented_not_occupied_units,
    dp04.for_sale_only_units,
    dp04.sold_not_occupied_units,
    dp04.seasonal_recreational_units,
    dp04.migrant_worker_units,
    dp04.other_vacant_units,

    -- Reliable demographic metrics from B03002
    b03002.b03002_001e AS total_population,
    b03002.b03002_002e AS not_hispanic_total,
    b03002.b03002_003e AS white_non_hispanic_population,
    b03002.b03002_004e AS black_non_hispanic_population,
    b03002.b03002_012e AS hispanic_population,

    CASE
        WHEN b03002.b03002_001e > 0
        THEN ROUND(100.0 * b03002.b03002_003e / b03002.b03002_001e, 2)
        ELSE NULL
    END AS pct_white_non_hispanic,

    CASE
        WHEN b03002.b03002_001e > 0
        THEN ROUND(100.0 * b03002.b03002_004e / b03002.b03002_001e, 2)
        ELSE NULL
    END AS pct_black_non_hispanic,

    CASE
        WHEN b03002.b03002_001e > 0
        THEN ROUND(100.0 * b03002.b03002_012e / b03002.b03002_001e, 2)
        ELSE NULL
    END AS pct_hispanic,

    CASE
        WHEN b19013.median_household_income IS NOT NULL
         AND b25003.occupied_units IS NOT NULL
         AND b25070.renter_hh_rent_burden_base IS NOT NULL
         AND dp04.housing_units_total IS NOT NULL
        THEN 1 ELSE 0
    END AS has_core_housing_metrics,

    CASE
        WHEN s1101.tract_geoid IS NOT NULL
         AND s1701.tract_geoid IS NOT NULL
         AND s1901.tract_geoid IS NOT NULL
         AND s2301.tract_geoid IS NOT NULL
        THEN 1 ELSE 0
    END AS has_batch2_metrics,

    CASE
        WHEN s1501.tract_geoid IS NOT NULL
         AND s2401.tract_geoid IS NOT NULL
         AND b03002.tract_geoid IS NOT NULL
         AND s0101.tract_geoid IS NOT NULL
         AND s1601.tract_geoid IS NOT NULL
        THEN 1 ELSE 0
    END AS has_batch3_metrics,

    CASE WHEN b19013.tract_geoid IS NOT NULL THEN to_jsonb(b19013) - ARRAY['year', 'tract_geoid', 'geo_id', 'name', 'statefp', 'countyfp', 'tractce', 'table_code', 'table_family', 'source_type'] ELSE NULL END AS b19013_metrics_json,
    CASE WHEN b25003.tract_geoid IS NOT NULL THEN to_jsonb(b25003) - ARRAY['year', 'tract_geoid', 'geo_id', 'name', 'statefp', 'countyfp', 'tractce', 'table_code', 'table_family', 'source_type'] ELSE NULL END AS b25003_metrics_json,
    CASE WHEN b25070.tract_geoid IS NOT NULL THEN to_jsonb(b25070) - ARRAY['year', 'tract_geoid', 'geo_id', 'name', 'statefp', 'countyfp', 'tractce', 'table_code', 'table_family', 'source_type'] ELSE NULL END AS b25070_metrics_json,
    CASE WHEN dp04.tract_geoid IS NOT NULL THEN to_jsonb(dp04) - ARRAY['year', 'tract_geoid', 'geo_id', 'name', 'statefp', 'countyfp', 'tractce', 'table_code', 'table_family', 'source_type'] ELSE NULL END AS dp04_metrics_json,
    CASE WHEN s1101.tract_geoid IS NOT NULL THEN to_jsonb(s1101) - ARRAY['year', 'tract_geoid', 'geo_id', 'name', 'statefp', 'countyfp', 'tractce', 'table_code', 'table_family', 'source_type'] ELSE NULL END AS s1101_metrics_json,
    CASE WHEN s1701.tract_geoid IS NOT NULL THEN to_jsonb(s1701) - ARRAY['year', 'tract_geoid', 'geo_id', 'name', 'statefp', 'countyfp', 'tractce', 'table_code', 'table_family', 'source_type'] ELSE NULL END AS s1701_metrics_json,
    CASE WHEN s1901.tract_geoid IS NOT NULL THEN to_jsonb(s1901) - ARRAY['year', 'tract_geoid', 'geo_id', 'name', 'statefp', 'countyfp', 'tractce', 'table_code', 'table_family', 'source_type'] ELSE NULL END AS s1901_metrics_json,
    CASE WHEN s2301.tract_geoid IS NOT NULL THEN to_jsonb(s2301) - ARRAY['year', 'tract_geoid', 'geo_id', 'name', 'statefp', 'countyfp', 'tractce', 'table_code', 'table_family', 'source_type'] ELSE NULL END AS s2301_metrics_json,
    CASE WHEN s1501.tract_geoid IS NOT NULL THEN to_jsonb(s1501) - ARRAY['year', 'tract_geoid', 'geo_id', 'name', 'statefp', 'countyfp', 'tractce', 'table_code', 'table_family', 'source_type'] ELSE NULL END AS s1501_metrics_json,
    CASE WHEN s2401.tract_geoid IS NOT NULL THEN to_jsonb(s2401) - ARRAY['year', 'tract_geoid', 'geo_id', 'name', 'statefp', 'countyfp', 'tractce', 'table_code', 'table_family', 'source_type'] ELSE NULL END AS s2401_metrics_json,
    CASE WHEN b03002.tract_geoid IS NOT NULL THEN to_jsonb(b03002) - ARRAY['year', 'tract_geoid', 'geo_id', 'name', 'statefp', 'countyfp', 'tractce', 'table_code', 'table_family', 'source_type'] ELSE NULL END AS b03002_metrics_json,
    CASE WHEN s0101.tract_geoid IS NOT NULL THEN to_jsonb(s0101) - ARRAY['year', 'tract_geoid', 'geo_id', 'name', 'statefp', 'countyfp', 'tractce', 'table_code', 'table_family', 'source_type'] ELSE NULL END AS s0101_metrics_json,
    CASE WHEN s1601.tract_geoid IS NOT NULL THEN to_jsonb(s1601) - ARRAY['year', 'tract_geoid', 'geo_id', 'name', 'statefp', 'countyfp', 'tractce', 'table_code', 'table_family', 'source_type'] ELSE NULL END AS s1601_metrics_json

FROM base
LEFT JOIN public.vw_int_acs_b19013_all_years b19013
  ON b19013.year = base.year
 AND b19013.tract_geoid = base.tract_geoid
LEFT JOIN public.vw_int_acs_b25003_all_years b25003
  ON b25003.year = base.year
 AND b25003.tract_geoid = base.tract_geoid
LEFT JOIN public.vw_int_acs_b25070_all_years b25070
  ON b25070.year = base.year
 AND b25070.tract_geoid = base.tract_geoid
LEFT JOIN public.vw_int_acs_dp04_all_years dp04
  ON dp04.year = base.year
 AND dp04.tract_geoid = base.tract_geoid

LEFT JOIN public.vw_int_acs_s1101_all_years s1101
  ON s1101.year = base.year
 AND s1101.tract_geoid = base.tract_geoid
LEFT JOIN public.vw_int_acs_s1701_all_years s1701
  ON s1701.year = base.year
 AND s1701.tract_geoid = base.tract_geoid
LEFT JOIN public.vw_int_acs_s1901_all_years s1901
  ON s1901.year = base.year
 AND s1901.tract_geoid = base.tract_geoid
LEFT JOIN public.vw_int_acs_s2301_all_years s2301
  ON s2301.year = base.year
 AND s2301.tract_geoid = base.tract_geoid

LEFT JOIN public.vw_int_acs_s1501_all_years s1501
  ON s1501.year = base.year
 AND s1501.tract_geoid = base.tract_geoid
LEFT JOIN public.vw_int_acs_s2401_all_years s2401
  ON s2401.year = base.year
 AND s2401.tract_geoid = base.tract_geoid
LEFT JOIN public.vw_int_acs_b03002_all_years b03002
  ON b03002.year = base.year
 AND b03002.tract_geoid = base.tract_geoid
LEFT JOIN public.vw_int_acs_s0101_all_years s0101
  ON s0101.year = base.year
 AND s0101.tract_geoid = base.tract_geoid
LEFT JOIN public.vw_int_acs_s1601_all_years s1601
  ON s1601.year = base.year
 AND s1601.tract_geoid = base.tract_geoid
;

CREATE INDEX idx_fact_acs_tract_profile_key
    ON public.fact_acs_tract_profile (year, tract_geoid);

CREATE INDEX idx_fact_acs_tract_profile_year
    ON public.fact_acs_tract_profile (year);

CREATE INDEX idx_fact_acs_tract_profile_stable
    ON public.fact_acs_tract_profile (is_stable_all_4_years, year);

CREATE VIEW public.vw_acs_tract_profile_all_years AS
SELECT *
FROM public.fact_acs_tract_profile;

CREATE VIEW public.vw_acs_tract_profile_2023 AS
SELECT *
FROM public.fact_acs_tract_profile
WHERE year = 2023;

CREATE VIEW public.vw_acs_tract_profile_stable_4yr AS
SELECT *
FROM public.fact_acs_tract_profile
WHERE is_stable_all_4_years = 1;

CREATE VIEW public.vw_acs_tract_profile_cluster_input AS
SELECT
    year,
    tract_geoid,
    tractce,
    tract_number,
    tract_name_canonical,
    is_stable_all_4_years,
    year_count,
    median_household_income,
    pct_owner_occupied,
    pct_renter_occupied,
    pct_rent_burden_30_plus,
    pct_rent_burden_50_plus,
    pct_vacant_housing_units,
    pct_white_non_hispanic,
    pct_black_non_hispanic,
    pct_hispanic,
    has_core_housing_metrics,
    has_batch2_metrics,
    has_batch3_metrics
FROM public.fact_acs_tract_profile;
