DROP TABLE IF EXISTS alice_household_base_2019;

CREATE TABLE alice_household_base_2019 AS
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
FROM alice_housing_2019_champaign;
