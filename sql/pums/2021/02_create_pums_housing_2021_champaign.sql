DROP TABLE IF EXISTS alice_housing_2021_champaign;

CREATE TABLE alice_housing_2021_champaign AS
SELECT
    *
FROM alice_housing_2021_raw
WHERE state = 17
  AND puma = 2100;
