create table alice_household_final_2023 as
select
    *,
    wgtp_adj as analysis_weight
from alice_household_profile_2023_adj;

