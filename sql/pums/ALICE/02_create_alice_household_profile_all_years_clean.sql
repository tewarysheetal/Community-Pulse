DROP TABLE IF EXISTS alice_household_profile_all_years_clean;

CREATE TABLE alice_household_profile_all_years_clean AS
SELECT *
FROM alice_household_profile_all_years
WHERE person_count IS NOT NULL
  AND np > 0
  AND hincp IS NOT NULL;