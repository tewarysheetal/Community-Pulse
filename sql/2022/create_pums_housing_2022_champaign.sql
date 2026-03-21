CREATE TABLE housing_2022_champaign AS
SELECT
    *
FROM housing_2022_raw
WHERE state = 17
  AND puma IN (1901, 1902);
