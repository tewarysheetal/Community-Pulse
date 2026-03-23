drop table if exists alice_nonstudent_vs_complete_profile_compare_2019;

create table alice_nonstudent_vs_complete_profile_compare_2019 as
with complete_profile as (
    select *
    from alice_below_alice_stat_profile_2019
),
nonstudent_profile as (
    select *
    from alice_nonstudent_stat_profile_2019
)
select
    coalesce(c.year, n.year)::integer as year,
    coalesce(c.metric_group, n.metric_group)::text as metric_group,
    coalesce(c.metric_name, n.metric_name)::text as metric_name,
    c.metric_value::numeric as complete_metric_value,
    n.metric_value::numeric as nonstudent_metric_value,
    round(coalesce(n.metric_value, 0) - coalesce(c.metric_value, 0), 4)::numeric as value_diff_nonstudent_minus_complete,
    c.pct_of_households::numeric as complete_pct_of_households,
    n.pct_of_households::numeric as nonstudent_pct_of_households,
    round(coalesce(n.pct_of_households, 0) - coalesce(c.pct_of_households, 0), 4)::numeric as pct_point_diff,
    round(
        100.0 * coalesce(n.metric_value, 0) / nullif(c.metric_value, 0),
        4
    )::numeric as nonstudent_as_pct_of_complete,
    coalesce(c.sort_order, n.sort_order)::integer as sort_order
from complete_profile c
full join nonstudent_profile n
  on n.metric_group = c.metric_group
 and n.metric_name = c.metric_name
order by sort_order, metric_group, metric_name;
