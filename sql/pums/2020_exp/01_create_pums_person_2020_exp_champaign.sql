DROP TABLE IF EXISTS alice_person_2020_exp_champaign;

CREATE TABLE alice_person_2020_exp_champaign AS
SELECT
    *
FROM "alice_person_2020_exp_raw"
WHERE state = 17
  AND puma = 2100;
