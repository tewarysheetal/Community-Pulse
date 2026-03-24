-- 1) Backup current table
DROP TABLE IF EXISTS public.fact_acs_tract_profile_v2_backup_before_s1101_fix;

CREATE TABLE public.fact_acs_tract_profile_v2_backup_before_s1101_fix AS
SELECT *
FROM public.fact_acs_tract_profile_v2;

-- 2) Correct the S1101 drift-sensitive fields
UPDATE public.fact_acs_tract_profile_v2 p
SET
    pct_households_with_under_18 =
        s.s1101_c01_010e,

    pct_households_with_60_plus =
        s.s1101_c01_011e,

    pct_households_with_65_plus =
        CASE
            WHEN p.year >= 2021 THEN s.s1101_c01_012e
            ELSE NULL
        END,

    pct_one_person_households =
        CASE
            WHEN p.year >= 2021 THEN s.s1101_c01_013e
            ELSE s.s1101_c01_012e
        END,

    pct_senior_living_alone_households =
        CASE
            WHEN p.year >= 2021 THEN s.s1101_c01_014e
            ELSE s.s1101_c01_013e
        END
FROM public.vw_int_acs_s1101_all_years s
WHERE p.year = s.year
  AND p.tract_geoid = s.tract_geoid;