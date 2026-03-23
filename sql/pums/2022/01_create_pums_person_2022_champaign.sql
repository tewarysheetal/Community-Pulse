DROP TABLE IF EXISTS alice_person_2022_champaign;

CREATE TABLE alice_person_2022_champaign AS
SELECT
    *
FROM alice_person_2022_raw
WHERE state = 17
  AND puma IN (1901, 1902);
