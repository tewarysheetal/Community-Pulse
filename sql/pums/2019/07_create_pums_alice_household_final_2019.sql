create table alice_household_final_2019 as
select
    *,
    wgtp::numeric as analysis_weight
from alice_household_profile_2019
where puma = 2100
  and coalesce(np, 0) > 0
  and coalesce(person_count, 0) > 0
  and hincp is not null;