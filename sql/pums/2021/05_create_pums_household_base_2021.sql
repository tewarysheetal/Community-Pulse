DROP TABLE IF EXISTS alice_household_base_2021;

CREATE TABLE alice_household_base_2021 AS
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
FROM alice_housing_2021_champaign;
