CREATE TABLE person_2020_exp_champaign AS
SELECT
    *
FROM "person_2020_exp_raw"
WHERE state = 17
  AND puma = 2100;
