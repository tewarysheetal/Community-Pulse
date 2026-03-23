DROP TABLE IF EXISTS alice_housing_2019_champaign;

CREATE TABLE alice_housing_2019_champaign AS
SELECT
    *
FROM alice_housing_2019_raw
WHERE state = 17
  AND puma = 2100;
