CREATE TABLE housing_2021_champaign AS
SELECT
    *
FROM housing_2021_raw
WHERE state = 17
  AND puma = 2100;
