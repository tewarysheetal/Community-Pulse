update alice_thresholds
set monthly_survival_budget = 2779,
    annual_survival_budget = 33348,
    annual_alice_threshold = 33348
where year = 2023
  and hh_comp_key = '1_adult_0_child';

update alice_thresholds
set monthly_survival_budget = 3824,
    annual_survival_budget = 45888,
    annual_alice_threshold = 45888
where year = 2023
  and hh_comp_key = '1_adult_1_child';

update alice_thresholds
set monthly_survival_budget = 5186,
    annual_survival_budget = 62232,
    annual_alice_threshold = 62232
where year = 2023
  and hh_comp_key = '1_adult_2plus_child';

update alice_thresholds
set monthly_survival_budget = 3874,
    annual_survival_budget = 46488,
    annual_alice_threshold = 46488
where year = 2023
  and hh_comp_key = '2_adult_0_child';

update alice_thresholds
set monthly_survival_budget = 5230,
    annual_survival_budget = 62760,
    annual_alice_threshold = 62760
where year = 2023
  and hh_comp_key = '2_adult_1_child';

update alice_thresholds
set monthly_survival_budget = 6001,
    annual_survival_budget = 72012,
    annual_alice_threshold = 72012
where year = 2023
  and hh_comp_key = '2_adult_2plus_child';

update alice_thresholds
set monthly_survival_budget = 5340,
    annual_survival_budget = 64080,
    annual_alice_threshold = 64080
where year = 2023
  and hh_comp_key = '3plus_adult_0_child';

update alice_thresholds
set monthly_survival_budget = 6108,
    annual_survival_budget = 73296,
    annual_alice_threshold = 73296
where year = 2023
  and hh_comp_key = '3plus_adult_1plus_child';

update alice_thresholds
set monthly_survival_budget = 2779,
    annual_survival_budget = 33348,
    annual_alice_threshold = 33348
where year = 2023
  and hh_comp_key = 'other';
