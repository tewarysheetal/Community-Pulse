DROP TABLE IF EXISTS alice_person_2021_champaign;

CREATE TABLE alice_person_2021_champaign AS
SELECT
    *
FROM alice_person_2021_raw
WHERE state = 17
  AND puma = 2100;
