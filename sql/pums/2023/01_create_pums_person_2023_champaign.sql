DROP TABLE IF EXISTS alice_person_2023_champaign;

CREATE TABLE alice_person_2023_champaign AS
SELECT
    *
FROM alice_person_2023_raw
WHERE state = 17
  AND puma IN (1901, 1902);
