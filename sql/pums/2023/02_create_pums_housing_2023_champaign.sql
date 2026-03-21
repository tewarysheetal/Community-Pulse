CREATE TABLE housing_2023_champaign AS
SELECT
    *
FROM housing_2023_raw
WHERE state = 17
  AND puma IN (1901, 1902);
