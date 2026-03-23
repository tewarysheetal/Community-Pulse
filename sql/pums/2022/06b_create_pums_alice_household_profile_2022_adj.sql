drop table if exists alice_household_profile_2022_adj;

create table alice_household_profile_2022_adj as
select
    a.*,
    case
        when a.puma = 1901 then a.wgtp::numeric
        when a.puma = 1902 then a.wgtp::numeric * p.alpha
        else null
    end as wgtp_adj
from alice_household_profile_2022 a
cross join (
    select alpha
    from alice_puma1902_alpha
    where alloc_year = 2022
) p
where a.puma in (1901, 1902)
  and coalesce(a.np, 0) > 0
  and coalesce(a.person_count, 0) > 0;
