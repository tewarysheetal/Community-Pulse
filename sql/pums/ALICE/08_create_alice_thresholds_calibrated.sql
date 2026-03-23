drop table if exists alice_thresholds_calibrated;

create table alice_thresholds_calibrated as
select *
from alice_thresholds;

update alice_thresholds_calibrated
set annual_alice_threshold = annual_alice_threshold + 5000,
    annual_survival_budget = annual_survival_budget + 5000,
    monthly_survival_budget = round((annual_survival_budget + 5000) / 12.0, 2),
    notes = coalesce(notes, '') || ' | calibrated +5000 for dominant childless buckets'
where year = 2019
  and hh_comp_key in ('1_adult_0_child', '2_adult_0_child');

update alice_thresholds_calibrated
set annual_alice_threshold = annual_alice_threshold + 7500,
    annual_survival_budget = annual_survival_budget + 7500,
    monthly_survival_budget = round((annual_survival_budget + 7500) / 12.0, 2),
    notes = coalesce(notes, '') || ' | calibrated +7500 for dominant childless buckets'
where year = 2021
  and hh_comp_key in ('1_adult_0_child', '2_adult_0_child');

update alice_thresholds_calibrated
set annual_alice_threshold = annual_alice_threshold + 7500,
    annual_survival_budget = annual_survival_budget + 7500,
    monthly_survival_budget = round((annual_survival_budget + 7500) / 12.0, 2),
    notes = coalesce(notes, '') || ' | calibrated +7500 for dominant childless buckets'
where year = 2022
  and hh_comp_key in ('1_adult_0_child', '2_adult_0_child');