DROP TABLE IF EXISTS alice_household_base_2020_exp;

CREATE TABLE alice_household_base_2020_exp AS
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
FROM alice_housing_2020_exp_champaign;
