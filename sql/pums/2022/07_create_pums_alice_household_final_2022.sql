drop table if exists alice_household_final_2022;

  create table alice_household_final_2022 as
  select
      *,
      wgtp_adj::numeric as analysis_weight
  from alice_household_profile_2022_adj
  where puma in (1901, 1902)
and coalesce(np, 0) > 0
and coalesce(person_count, 0) > 0
and hincp is not null;
