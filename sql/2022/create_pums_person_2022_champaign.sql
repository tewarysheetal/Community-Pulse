CREATE TABLE person_2022_champaign AS
SELECT
    *
FROM person_2022_raw
WHERE state = 17
  AND puma IN (1901, 1902);
