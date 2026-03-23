drop table if exists alice_nonstudent_households_2021;

create table alice_nonstudent_households_2021 as
select *
from alice_household_final_2021
where below_alice_flag = 1
  and coalesce(student_heavy_flag, 0) = 0;
