drop table if exists alice_below_alice_households_2019;

create table alice_below_alice_households_2019 as
select
    2019::integer as year,
    t.*
from alice_household_final_2019 t
where below_alice_flag = 1;
