select
    hh_comp_key,
    round(sum(analysis_weight), 0) as households
from alice_nonstudent_households_2023
group by hh_comp_key
order by households desc;

select
    case when noc > 0 then 'with_children' else 'no_children' end as child_group,
    round(sum(analysis_weight), 0) as households
from alice_nonstudent_households_2023
group by 1
order by households desc;

select
    case
        when employed_count = 0 then 'no_employed_person'
        when employed_count = 1 then 'one_employed_person'
        else 'two_or_more_employed'
    end as employment_group,
    round(sum(analysis_weight), 0) as households
from alice_nonstudent_households_2023
group by 1
order by households desc;