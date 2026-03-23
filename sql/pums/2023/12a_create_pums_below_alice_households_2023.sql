drop table if exists alice_below_alice_households_2023;

create table alice_below_alice_households_2023 as
select
    2023::integer as year,
    t.*
from alice_household_final_2023 t
where below_alice_flag = 1;
