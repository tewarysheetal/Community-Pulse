drop table if exists alice_nonstudent_households_2023;

create table alice_nonstudent_households_2023 as
select *
from alice_household_final_2023
where below_alice_flag = 1
  and coalesce(student_heavy_flag, 0) = 0;
