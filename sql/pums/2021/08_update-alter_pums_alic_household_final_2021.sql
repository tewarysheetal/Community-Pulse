alter table alice_household_final_2021 add column if not exists hincp_adj_real numeric;
update alice_household_final_2021
set hincp_adj_real = (hincp::numeric * adjinc::numeric) / 1000000.0;

alter table alice_household_final_2021 add column if not exists student_heavy_flag integer;
update alice_household_final_2021
set student_heavy_flag = coalesce(student_heavy_flag_rule_b, 0);

alter table alice_household_final_2021
add column if not exists hh_comp_key text;

update alice_household_final_2021
set hh_comp_key =
    case
        when adult_count = 1 and noc = 0 then '1_adult_0_child'
        when adult_count = 1 and noc = 1 then '1_adult_1_child'
        when adult_count = 1 and noc >= 2 then '1_adult_2plus_child'
        when adult_count = 2 and noc = 0 then '2_adult_0_child'
        when adult_count = 2 and noc = 1 then '2_adult_1_child'
        when adult_count = 2 and noc >= 2 then '2_adult_2plus_child'
        when adult_count >= 3 and noc = 0 then '3plus_adult_0_child'
        when adult_count >= 3 and noc >= 1 then '3plus_adult_1plus_child'
        else 'other'
    end;