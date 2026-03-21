CREATE TABLE person_2019_champaign AS
SELECT
    *
FROM person_2019_raw
WHERE state = 17
  AND puma = 2100;
