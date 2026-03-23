SELECT COUNT(*) AS dim_tract_rows
FROM dim_tract;

SELECT COUNT(*) AS stable_4_year_tracts
FROM dim_tract
WHERE is_stable_all_4_years = 1;

SELECT year, COUNT(*) AS tract_count
FROM bridge_tract_year
GROUP BY year
ORDER BY year;

SELECT *
FROM dim_year
ORDER BY year;