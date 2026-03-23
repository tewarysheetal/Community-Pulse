DROP TABLE IF EXISTS alice_housing_2022_champaign;

CREATE TABLE alice_housing_2022_champaign AS
SELECT
    *
FROM alice_housing_2022_raw
WHERE state = 17
  AND puma IN (1901, 1902);
