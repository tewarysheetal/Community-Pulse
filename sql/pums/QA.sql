Select '2019' As year, count(*) From household_base_2019
union 
Select '2020' As year, count(*) From household_base_2020_exp
union
Select '2021' As year, count(*) From household_base_2021
union
Select '2022' As year, count(*) From household_base_2022
union
Select '2023' As year, count(*) From household_base_2023;

SELECT '2019' AS yr, COUNT(*) FROM alice_household_profile_2019 WHERE person_count IS NULL
UNION ALL
SELECT '2020_exp', COUNT(*) FROM alice_household_profile_2020_exp WHERE person_count IS NULL
UNION ALL
SELECT '2021', COUNT(*) FROM alice_household_profile_2021 WHERE person_count IS NULL
UNION ALL
SELECT '2022', COUNT(*) FROM alice_household_profile_2022 WHERE person_count IS NULL
UNION ALL
SELECT '2023', COUNT(*) FROM alice_household_profile_2023 WHERE person_count IS NULL;

SELECT year, COUNT(*) AS row_count
FROM alice_household_profile_all_years
GROUP BY year
ORDER BY year;

SELECT
    year,
    COUNT(*) AS sample_households,
    SUM(wgtp) AS weighted_households,
    SUM(CASE WHEN person_count IS NULL THEN 1 ELSE 0 END) AS null_person_rows,
    SUM(CASE WHEN person_count IS NULL THEN wgtp ELSE 0 END) AS weighted_null_person_households
FROM alice_household_profile_all_years
GROUP BY year
ORDER BY year;

SELECT
    year,
    COUNT(*) AS null_person_rows,
    AVG(np) AS avg_np,
    SUM(CASE WHEN np = 0 THEN 1 ELSE 0 END) AS np_zero_count,
    SUM(CASE WHEN hincp IS NULL THEN 1 ELSE 0 END) AS hincp_null_count
FROM alice_household_profile_all_years
WHERE person_count IS NULL
GROUP BY year
ORDER BY year;

SELECT
    year,
    COUNT(*) AS sample_households,
    SUM(wgtp) AS weighted_households
FROM alice_household_profile_all_years_clean
GROUP BY year
ORDER BY year;

SELECT
    year,
    puma,
    COUNT(*) AS sample_households,
    SUM(wgtp) AS weighted_households
FROM alice_household_profile_all_years_clean
WHERE year IN (2022, 2023)
GROUP BY year, puma
ORDER BY year, puma;