drop table if exists alice_household_final_2021;

  create table alice_household_final_2021 as
  select
      *,
      wgtp::numeric as analysis_weight
  from alice_household_profile_2021
  where puma = 2100
and coalesce(np, 0) > 0
and coalesce(person_count, 0) > 0
and hincp is not null;
