update alice_thresholds t
set monthly_survival_budget = round((b.annual_survival_budget * i.ratio_to_2023) / 12.0, 2),
    annual_survival_budget = round(b.annual_survival_budget * i.ratio_to_2023, 2),
    annual_alice_threshold = round(b.annual_alice_threshold * i.ratio_to_2023, 2)
from alice_thresholds b
join illinois_essentials_index i
  on i.year = 2021
where b.year = 2023
  and b.hh_comp_key = t.hh_comp_key
  and t.year = 2021;
