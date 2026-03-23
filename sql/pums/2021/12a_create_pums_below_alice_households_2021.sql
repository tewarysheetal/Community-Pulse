drop table if exists alice_below_alice_households_2021;

create table alice_below_alice_households_2021 as
select
    2021::integer as year,
    t.*
from alice_household_final_2021 t
where below_alice_flag = 1;
