CREATE TABLE person_2021_champaign AS
SELECT
    *
FROM person_2021_raw
WHERE state = 17
  AND puma = 2100;
