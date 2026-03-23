alter table alice_household_final_2023
    add column if not exists below_alice_flag integer;


alter table alice_household_final_2023
add column if not exists annual_alice_threshold numeric;

update alice_household_final_2023 h
set annual_alice_threshold = t.annual_alice_threshold
from alice_thresholds_calibrated t
where t.year = 2023
  and t.hh_comp_key = h.hh_comp_key;


    update alice_household_final_2023 h
    set below_alice_flag =
        case
            when h.annual_alice_threshold is null then null
            when h.hincp_adj_real < h.annual_alice_threshold then 1
            else 0
        end;
