CREATE TABLE housing_2019_champaign AS
SELECT
    *
FROM housing_2019_raw
WHERE state = 17
  AND puma = 2100;
