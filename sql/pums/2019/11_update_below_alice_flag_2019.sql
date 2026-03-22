alter table alice_household_final_2019
    add column if not exists below_alice_flag integer;


alter table alice_household_final_2019
add column if not exists annual_alice_threshold numeric;

update alice_household_final_2019 h
set annual_alice_threshold = t.annual_alice_threshold
from alice_thresholds t
where t.year = 2019
  and t.hh_comp_key = h.hh_comp_key;


    update alice_household_final_2019 h
    set below_alice_flag =
        case
            when h.annual_alice_threshold is null then null
            when h.hincp_adj_real < h.annual_alice_threshold then 1
            else 0
        end;


-- Validation check for 2019
-- stored_flag_weighted and direct_compare_weighted should match
select
    2019 as year,
    round(sum(case when below_alice_flag = 1 then analysis_weight else 0 end), 0) as stored_flag_weighted,
    round(sum(case when hincp_adj_real < annual_alice_threshold then analysis_weight else 0 end), 0) as direct_compare_weighted
from alice_household_final_2019;
