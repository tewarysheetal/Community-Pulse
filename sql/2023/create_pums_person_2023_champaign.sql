CREATE TABLE person_2023_champaign AS
SELECT
    *
FROM person_2023_raw
WHERE state = 17
  AND puma IN (1901, 1902);
