drop table if exists alice_below_alice_households_2022;

create table alice_below_alice_households_2022 as
select
    2022::integer as year,
    t.*
from alice_household_final_2022 t
where below_alice_flag = 1;
