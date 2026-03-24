
DROP VIEW IF EXISTS public.vw_acs_tract_profile_v2_cluster_input;
DROP VIEW IF EXISTS public.vw_acs_tract_profile_v2_stable_4yr;
DROP VIEW IF EXISTS public.vw_acs_tract_profile_v2_2023;
DROP VIEW IF EXISTS public.vw_acs_tract_profile_v2_all_years;

DROP TABLE IF EXISTS public.fact_acs_tract_profile_v2;

CREATE TABLE public.fact_acs_tract_profile_v2 AS
SELECT
    p.*,

    -- Age from S0101
    s0101.s0101_c01_022e AS age_under_18_population,
    s0101.s0101_c01_023e AS age_18_24_population,
    s0101.s0101_c01_030e AS age_65_plus_population,
    s0101.s0101_c02_022e AS pct_age_under_18,
    s0101.s0101_c02_023e AS pct_age_18_24,
    s0101.s0101_c02_030e AS pct_age_65_plus,

    -- Household composition from S1101
    s1101.s1101_c01_001e AS total_households_s1101,
    s1101.s1101_c01_002e AS avg_household_size_v2,
    s1101.s1101_c01_003e AS total_families_s1101,
    s1101.s1101_c01_005e AS households_with_own_children_under_18,
    s1101.s1101_c01_010e AS households_with_one_or_more_under_18,
    s1101.s1101_c01_012e AS households_with_65_plus,
    s1101.s1101_c01_013e AS householders_living_alone,
    s1101.s1101_c01_014e AS householders_living_alone_65_plus,

    CASE
        WHEN s1101.s1101_c01_001e > 0
        THEN ROUND(100.0 * s1101.s1101_c01_003e / s1101.s1101_c01_001e, 2)
        ELSE NULL
    END AS pct_family_households,

    CASE
        WHEN s1101.s1101_c01_001e > 0
        THEN ROUND(100.0 - (100.0 * s1101.s1101_c01_003e / s1101.s1101_c01_001e), 2)
        ELSE NULL
    END AS pct_nonfamily_households,

    CASE
        WHEN s1101.s1101_c01_001e > 0
        THEN ROUND(100.0 * s1101.s1101_c01_005e / s1101.s1101_c01_001e, 2)
        ELSE NULL
    END AS pct_households_with_own_children_under_18,

    CASE
        WHEN s1101.s1101_c01_001e > 0
        THEN ROUND(100.0 * s1101.s1101_c01_010e / s1101.s1101_c01_001e, 2)
        ELSE NULL
    END AS pct_households_with_under_18,

    CASE
        WHEN s1101.s1101_c01_001e > 0
        THEN ROUND(100.0 * s1101.s1101_c01_012e / s1101.s1101_c01_001e, 2)
        ELSE NULL
    END AS pct_households_with_65_plus,

    CASE
        WHEN s1101.s1101_c01_001e > 0
        THEN ROUND(100.0 * s1101.s1101_c01_013e / s1101.s1101_c01_001e, 2)
        ELSE NULL
    END AS pct_one_person_households,

    CASE
        WHEN s1101.s1101_c01_001e > 0
        THEN ROUND(100.0 * s1101.s1101_c01_014e / s1101.s1101_c01_001e, 2)
        ELSE NULL
    END AS pct_senior_living_alone_households,

    -- Education from S1501
    CASE
        WHEN s1501.s1501_c02_014e IS NOT NULL
        THEN ROUND(100.0 - s1501.s1501_c02_014e, 2)
        ELSE NULL
    END AS pct_less_than_high_school,

    s1501.s1501_c02_009e AS pct_high_school_grad,
    s1501.s1501_c02_010e AS pct_some_college_no_degree,
    s1501.s1501_c02_011e AS pct_associates_degree,

    CASE
        WHEN s1501.s1501_c02_010e IS NOT NULL
          OR s1501.s1501_c02_011e IS NOT NULL
        THEN ROUND(COALESCE(s1501.s1501_c02_010e, 0) + COALESCE(s1501.s1501_c02_011e, 0), 2)
        ELSE NULL
    END AS pct_some_college_or_associate,

    s1501.s1501_c02_012e AS pct_bachelors_degree,
    s1501.s1501_c02_013e AS pct_graduate_or_professional,
    s1501.s1501_c02_015e AS pct_bachelors_or_higher,

    -- Poverty / employment
    s1701.s1701_c01_001e AS poverty_base_population,
    s1701.s1701_c03_001e AS poverty_rate,

    s2301.s2301_c02_001e AS labor_force_participation_rate,
    s2301.s2301_c03_001e AS employment_population_ratio,
    s2301.s2301_c04_001e AS unemployment_rate,

    -- Income distribution from S1901
    s1901.s1901_c01_001e AS total_households_s1901,

    ROUND(
        COALESCE(s1901.s1901_c01_002e, 0) +
        COALESCE(s1901.s1901_c01_003e, 0) +
        COALESCE(s1901.s1901_c01_004e, 0), 2
    ) AS pct_hh_income_under_25k,

    ROUND(
        COALESCE(s1901.s1901_c01_005e, 0) +
        COALESCE(s1901.s1901_c01_006e, 0), 2
    ) AS pct_hh_income_25k_50k,

    ROUND(
        COALESCE(s1901.s1901_c01_007e, 0) +
        COALESCE(s1901.s1901_c01_008e, 0), 2
    ) AS pct_hh_income_50k_100k,

    ROUND(
        COALESCE(s1901.s1901_c01_009e, 0) +
        COALESCE(s1901.s1901_c01_010e, 0) +
        COALESCE(s1901.s1901_c01_011e, 0), 2
    ) AS pct_hh_income_100k_plus,

    s1901.s1901_c01_012e AS median_household_income_s1901,
    s1901.s1901_c01_013e AS mean_household_income_s1901,

    -- Language context from S1601
    s1601.s1601_c02_003e AS pct_speak_language_other_than_english_at_home,

    CASE
        WHEN s0101.tract_geoid IS NOT NULL
         AND s1101.tract_geoid IS NOT NULL
         AND s1501.tract_geoid IS NOT NULL
         AND s1701.tract_geoid IS NOT NULL
         AND s1901.tract_geoid IS NOT NULL
         AND s2301.tract_geoid IS NOT NULL
        THEN 1 ELSE 0
    END AS has_v2_core_metrics

FROM public.fact_acs_tract_profile p
LEFT JOIN public.vw_int_acs_s0101_all_years s0101
  ON s0101.year = p.year
 AND s0101.tract_geoid = p.tract_geoid
LEFT JOIN public.vw_int_acs_s1101_all_years s1101
  ON s1101.year = p.year
 AND s1101.tract_geoid = p.tract_geoid
LEFT JOIN public.vw_int_acs_s1501_all_years s1501
  ON s1501.year = p.year
 AND s1501.tract_geoid = p.tract_geoid
LEFT JOIN public.vw_int_acs_s1701_all_years s1701
  ON s1701.year = p.year
 AND s1701.tract_geoid = p.tract_geoid
LEFT JOIN public.vw_int_acs_s1901_all_years s1901
  ON s1901.year = p.year
 AND s1901.tract_geoid = p.tract_geoid
LEFT JOIN public.vw_int_acs_s2301_all_years s2301
  ON s2301.year = p.year
 AND s2301.tract_geoid = p.tract_geoid
LEFT JOIN public.vw_int_acs_s1601_all_years s1601
  ON s1601.year = p.year
 AND s1601.tract_geoid = p.tract_geoid
;

CREATE INDEX idx_fact_acs_tract_profile_v2_key
    ON public.fact_acs_tract_profile_v2 (year, tract_geoid);

CREATE INDEX idx_fact_acs_tract_profile_v2_year
    ON public.fact_acs_tract_profile_v2 (year);

CREATE INDEX idx_fact_acs_tract_profile_v2_stable
    ON public.fact_acs_tract_profile_v2 (is_stable_all_4_years, year);

CREATE VIEW public.vw_acs_tract_profile_v2_all_years AS
SELECT *
FROM public.fact_acs_tract_profile_v2;

CREATE VIEW public.vw_acs_tract_profile_v2_2023 AS
SELECT *
FROM public.fact_acs_tract_profile_v2
WHERE year = 2023;

CREATE VIEW public.vw_acs_tract_profile_v2_stable_4yr AS
SELECT *
FROM public.fact_acs_tract_profile_v2
WHERE is_stable_all_4_years = 1;

CREATE VIEW public.vw_acs_tract_profile_v2_cluster_input AS
SELECT
    year,
    tract_geoid,
    tractce,
    tract_number,
    tract_name_canonical,
    is_stable_all_4_years,
    year_count,
    median_household_income,
    pct_rent_burden_30_plus,
    pct_rent_burden_50_plus,
    pct_owner_occupied,
    pct_renter_occupied,
    pct_vacant_housing_units,
    poverty_rate,
    unemployment_rate,
    labor_force_participation_rate,
    employment_population_ratio,
    pct_hh_income_under_25k,
    pct_hh_income_25k_50k,
    pct_hh_income_50k_100k,
    pct_hh_income_100k_plus,
    pct_less_than_high_school,
    pct_high_school_grad,
    pct_some_college_or_associate,
    pct_bachelors_or_higher,
    pct_age_under_18,
    pct_age_18_24,
    pct_age_65_plus,
    pct_family_households,
    pct_households_with_under_18,
    pct_households_with_65_plus,
    pct_one_person_households,
    pct_speak_language_other_than_english_at_home,
    pct_white_non_hispanic,
    pct_black_non_hispanic,
    pct_hispanic,
    has_core_housing_metrics,
    has_batch2_metrics,
    has_batch3_metrics,
    has_v2_core_metrics
FROM public.fact_acs_tract_profile_v2;
