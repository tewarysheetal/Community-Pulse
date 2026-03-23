DROP TABLE IF EXISTS alice_household_base_2023;

CREATE TABLE alice_household_base_2023 AS
SELECT
    serialno,
    state,
    puma,
    wgtp,
    np,
    hincp,
    fincp,
    adjinc,
    hht,
    hht2,
    noc,
    npf,
    fparc
FROM alice_housing_2023_champaign;
