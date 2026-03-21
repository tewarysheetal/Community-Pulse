CREATE TABLE alice_household_profile_all_years AS
SELECT
    2019 AS year,
    0 AS is_experimental,
    *
FROM alice_household_profile_2019

UNION ALL

SELECT
    2020 AS year,
    1 AS is_experimental,
    *
FROM alice_household_profile_2020_exp

UNION ALL

SELECT
    2021 AS year,
    0 AS is_experimental,
    *
FROM alice_household_profile_2021

UNION ALL

SELECT
    2022 AS year,
    0 AS is_experimental,
    *
FROM alice_household_profile_2022

UNION ALL

SELECT
    2023 AS year,
    0 AS is_experimental,
    *
FROM alice_household_profile_2023;