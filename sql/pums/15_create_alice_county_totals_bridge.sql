DROP TABLE IF EXISTS alice_county_totals_bridge;

        CREATE TABLE alice_county_totals_bridge AS
        WITH base AS (
            SELECT
    2019::integer AS year,
    'complete_calibrated'::text AS source_variant,
    (
        SELECT metric_value::numeric
        FROM alice_below_alice_stat_profile_2019
        WHERE metric_group = 'overall'
          AND metric_name = 'weighted_households'
        LIMIT 1
    ) AS alice_households,
    (
        SELECT COALESCE(SUM(analysis_weight::numeric), 0)
        FROM alice_household_final_2019
    ) AS county_total_households
UNION ALL
SELECT
    2019::integer AS year,
    'nonstudent_calibrated'::text AS source_variant,
    (
        SELECT metric_value::numeric
        FROM alice_nonstudent_stat_profile_2019
        WHERE metric_group = 'overall'
          AND metric_name = 'weighted_households'
        LIMIT 1
    ) AS alice_households,
    (
        SELECT COALESCE(SUM(analysis_weight::numeric), 0)
        FROM alice_household_final_2019
    ) AS county_total_households
UNION ALL
SELECT
    2021::integer AS year,
    'complete_calibrated'::text AS source_variant,
    (
        SELECT metric_value::numeric
        FROM alice_below_alice_stat_profile_2021
        WHERE metric_group = 'overall'
          AND metric_name = 'weighted_households'
        LIMIT 1
    ) AS alice_households,
    (
        SELECT COALESCE(SUM(analysis_weight::numeric), 0)
        FROM alice_household_final_2021
    ) AS county_total_households
UNION ALL
SELECT
    2021::integer AS year,
    'nonstudent_calibrated'::text AS source_variant,
    (
        SELECT metric_value::numeric
        FROM alice_nonstudent_stat_profile_2021
        WHERE metric_group = 'overall'
          AND metric_name = 'weighted_households'
        LIMIT 1
    ) AS alice_households,
    (
        SELECT COALESCE(SUM(analysis_weight::numeric), 0)
        FROM alice_household_final_2021
    ) AS county_total_households
UNION ALL
SELECT
    2022::integer AS year,
    'complete_calibrated'::text AS source_variant,
    (
        SELECT metric_value::numeric
        FROM alice_below_alice_stat_profile_2022
        WHERE metric_group = 'overall'
          AND metric_name = 'weighted_households'
        LIMIT 1
    ) AS alice_households,
    (
        SELECT COALESCE(SUM(analysis_weight::numeric), 0)
        FROM alice_household_final_2022
    ) AS county_total_households
UNION ALL
SELECT
    2022::integer AS year,
    'nonstudent_calibrated'::text AS source_variant,
    (
        SELECT metric_value::numeric
        FROM alice_nonstudent_stat_profile_2022
        WHERE metric_group = 'overall'
          AND metric_name = 'weighted_households'
        LIMIT 1
    ) AS alice_households,
    (
        SELECT COALESCE(SUM(analysis_weight::numeric), 0)
        FROM alice_household_final_2022
    ) AS county_total_households
UNION ALL
SELECT
    2023::integer AS year,
    'complete_calibrated'::text AS source_variant,
    (
        SELECT metric_value::numeric
        FROM alice_below_alice_stat_profile_2023
        WHERE metric_group = 'overall'
          AND metric_name = 'weighted_households'
        LIMIT 1
    ) AS alice_households,
    (
        SELECT COALESCE(SUM(analysis_weight::numeric), 0)
        FROM alice_household_final_2023
    ) AS county_total_households
UNION ALL
SELECT
    2023::integer AS year,
    'nonstudent_calibrated'::text AS source_variant,
    (
        SELECT metric_value::numeric
        FROM alice_nonstudent_stat_profile_2023
        WHERE metric_group = 'overall'
          AND metric_name = 'weighted_households'
        LIMIT 1
    ) AS alice_households,
    (
        SELECT COALESCE(SUM(analysis_weight::numeric), 0)
        FROM alice_household_final_2023
    ) AS county_total_households
        )
        SELECT
            year,
            source_variant,
            alice_households,
            county_total_households,
            ROUND(
                100.0 * alice_households / NULLIF(county_total_households, 0),
                4
            )::numeric AS alice_rate_pct
        FROM base
        ORDER BY year, source_variant;
