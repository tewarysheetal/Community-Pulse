drop table if exists alice_nonstudent_households_2019;

create table alice_nonstudent_households_2019 as
select *
from alice_household_final_2019
where below_alice_flag = 1
  and student_heavy_flag = 0;
