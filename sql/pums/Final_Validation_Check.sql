/* =========================================================
   FINAL VALIDATION PACK — CALIBRATED PIPELINE
   ========================================================= */

/* ---------------------------------------------------------
   1) Confirm final tables are using alice_thresholds_calibrated
   Pass condition: threshold_mismatch_rows = 0 for all years
   --------------------------------------------------------- */
with final_thresholds as (
    select 2019 as year, hh_comp_key, annual_alice_threshold from alice_household_final_2019
    union all
    select 2021 as year, hh_comp_key, annual_alice_threshold from alice_household_final_2021
    union all
    select 2022 as year, hh_comp_key, annual_alice_threshold from alice_household_final_2022
    union all
    select 2023 as year, hh_comp_key, annual_alice_threshold from alice_household_final_2023
)
select
    '01_calibrated_threshold_match' as validation_name,
    f.year,
    count(*) as row_count,
    count(*) filter (
        where f.annual_alice_threshold is null
           or abs(f.annual_alice_threshold - c.annual_alice_threshold) > 0.01
    ) as threshold_mismatch_rows
from final_thresholds f
join alice_thresholds_calibrated c
  on c.year = f.year
 and c.hh_comp_key = f.hh_comp_key
group by f.year
order by f.year;


/* ---------------------------------------------------------
   2) Spot-check the calibrated buckets vs raw thresholds
   You should see:
   2019: +5000 for 1_adult_0_child, 2_adult_0_child
   2021: +7500 for 1_adult_0_child, 2_adult_0_child
   2022: +7500 for 1_adult_0_child, 2_adult_0_child
   2023: no change
   --------------------------------------------------------- */
select
    '02_calibration_spot_check' as validation_name,
    r.year,
    r.hh_comp_key,
    r.annual_alice_threshold as raw_threshold,
    c.annual_alice_threshold as calibrated_threshold,
    c.annual_alice_threshold - r.annual_alice_threshold as uplift
from alice_thresholds r
join alice_thresholds_calibrated c
  on c.year = r.year
 and c.hh_comp_key = r.hh_comp_key
where r.hh_comp_key in ('1_adult_0_child', '2_adult_0_child')
order by r.year, r.hh_comp_key;


/* ---------------------------------------------------------
   3) Final-table null / completeness check
   Pass condition: all null_* columns = 0
   --------------------------------------------------------- */
select
    '03_final_table_null_check' as validation_name,
    2019 as year,
    count(*) as row_count,
    count(*) filter (where analysis_weight is null) as null_analysis_weight,
    count(*) filter (where hincp_adj_real is null) as null_hincp_adj_real,
    count(*) filter (where hh_comp_key is null) as null_hh_comp_key,
    count(*) filter (where annual_alice_threshold is null) as null_annual_alice_threshold,
    count(*) filter (where below_alice_flag is null) as null_below_alice_flag,
    count(*) filter (where student_heavy_flag is null) as null_student_heavy_flag
from alice_household_final_2019

union all

select
    '03_final_table_null_check' as validation_name,
    2021 as year,
    count(*) as row_count,
    count(*) filter (where analysis_weight is null) as null_analysis_weight,
    count(*) filter (where hincp_adj_real is null) as null_hincp_adj_real,
    count(*) filter (where hh_comp_key is null) as null_hh_comp_key,
    count(*) filter (where annual_alice_threshold is null) as null_annual_alice_threshold,
    count(*) filter (where below_alice_flag is null) as null_below_alice_flag,
    count(*) filter (where student_heavy_flag is null) as null_student_heavy_flag
from alice_household_final_2021

union all

select
    '03_final_table_null_check' as validation_name,
    2022 as year,
    count(*) as row_count,
    count(*) filter (where analysis_weight is null) as null_analysis_weight,
    count(*) filter (where hincp_adj_real is null) as null_hincp_adj_real,
    count(*) filter (where hh_comp_key is null) as null_hh_comp_key,
    count(*) filter (where annual_alice_threshold is null) as null_annual_alice_threshold,
    count(*) filter (where below_alice_flag is null) as null_below_alice_flag,
    count(*) filter (where student_heavy_flag is null) as null_student_heavy_flag
from alice_household_final_2022

union all

select
    '03_final_table_null_check' as validation_name,
    2023 as year,
    count(*) as row_count,
    count(*) filter (where analysis_weight is null) as null_analysis_weight,
    count(*) filter (where hincp_adj_real is null) as null_hincp_adj_real,
    count(*) filter (where hh_comp_key is null) as null_hh_comp_key,
    count(*) filter (where annual_alice_threshold is null) as null_annual_alice_threshold,
    count(*) filter (where below_alice_flag is null) as null_below_alice_flag,
    count(*) filter (where student_heavy_flag is null) as null_student_heavy_flag
from alice_household_final_2023

order by year;


/* ---------------------------------------------------------
   4) Internal balance check
   Pass condition: balance_check = 0
   --------------------------------------------------------- */
with actuals as (
    select
        2019 as year,
        sum(analysis_weight) as total_households,
        sum(case when below_alice_flag = 1 then analysis_weight else 0 end) as below_alice_households,
        sum(case when below_alice_flag = 0 then analysis_weight else 0 end) as above_alice_households
    from alice_household_final_2019

    union all

    select
        2021 as year,
        sum(analysis_weight) as total_households,
        sum(case when below_alice_flag = 1 then analysis_weight else 0 end) as below_alice_households,
        sum(case when below_alice_flag = 0 then analysis_weight else 0 end) as above_alice_households
    from alice_household_final_2021

    union all

    select
        2022 as year,
        sum(analysis_weight) as total_households,
        sum(case when below_alice_flag = 1 then analysis_weight else 0 end) as below_alice_households,
        sum(case when below_alice_flag = 0 then analysis_weight else 0 end) as above_alice_households
    from alice_household_final_2022

    union all

    select
        2023 as year,
        sum(analysis_weight) as total_households,
        sum(case when below_alice_flag = 1 then analysis_weight else 0 end) as below_alice_households,
        sum(case when below_alice_flag = 0 then analysis_weight else 0 end) as above_alice_households
    from alice_household_final_2023
)
select
    '04_balance_check' as validation_name,
    year,
    round(total_households, 0) as total_households,
    round(below_alice_households, 0) as below_alice_households,
    round(above_alice_households, 0) as above_alice_households,
    round(total_households - (below_alice_households + above_alice_households), 4) as balance_check
from actuals
order by year;


/* ---------------------------------------------------------
   5) Benchmark comparison vs the sheet targets you used earlier
   Main readout:
   - total households should be close
   - below ALICE should improve vs raw-threshold run
   - above ALICE should stay coherent
   --------------------------------------------------------- */
with targets as (
    select * from (
        values
            (2019, 81764::numeric, 36933::numeric, 44831::numeric),
            (2021, 84248::numeric, 37298::numeric, 46950::numeric),
            (2022, 84713::numeric, 39534::numeric, 45179::numeric),
            (2023, 89155::numeric, 39243::numeric, 49912::numeric)
    ) as t(year, households_sheet, below_alice_sheet, above_alice_sheet)
),
actuals as (
    select
        2019 as year,
        sum(analysis_weight) as households_actual,
        sum(case when below_alice_flag = 1 then analysis_weight else 0 end) as below_alice_actual,
        sum(case when below_alice_flag = 0 then analysis_weight else 0 end) as above_alice_actual
    from alice_household_final_2019

    union all

    select
        2021 as year,
        sum(analysis_weight) as households_actual,
        sum(case when below_alice_flag = 1 then analysis_weight else 0 end) as below_alice_actual,
        sum(case when below_alice_flag = 0 then analysis_weight else 0 end) as above_alice_actual
    from alice_household_final_2021

    union all

    select
        2022 as year,
        sum(analysis_weight) as households_actual,
        sum(case when below_alice_flag = 1 then analysis_weight else 0 end) as below_alice_actual,
        sum(case when below_alice_flag = 0 then analysis_weight else 0 end) as above_alice_actual
    from alice_household_final_2022

    union all

    select
        2023 as year,
        sum(analysis_weight) as households_actual,
        sum(case when below_alice_flag = 1 then analysis_weight else 0 end) as below_alice_actual,
        sum(case when below_alice_flag = 0 then analysis_weight else 0 end) as above_alice_actual
    from alice_household_final_2023
)
select
    '05_sheet_compare_total' as validation_name,
    a.year,
    round(a.households_actual, 0) as actual_value,
    t.households_sheet as target_value,
    round(a.households_actual - t.households_sheet, 0) as diff,
    round(100.0 * (a.households_actual - t.households_sheet) / nullif(t.households_sheet, 0), 2) as pct_diff
from actuals a
join targets t on t.year = a.year

union all

select
    '06_sheet_compare_below_alice' as validation_name,
    a.year,
    round(a.below_alice_actual, 0) as actual_value,
    t.below_alice_sheet as target_value,
    round(a.below_alice_actual - t.below_alice_sheet, 0) as diff,
    round(100.0 * (a.below_alice_actual - t.below_alice_sheet) / nullif(t.below_alice_sheet, 0), 2) as pct_diff
from actuals a
join targets t on t.year = a.year

union all

select
    '07_sheet_compare_above_alice' as validation_name,
    a.year,
    round(a.above_alice_actual, 0) as actual_value,
    t.above_alice_sheet as target_value,
    round(a.above_alice_actual - t.above_alice_sheet, 0) as diff,
    round(100.0 * (a.above_alice_actual - t.above_alice_sheet) / nullif(t.above_alice_sheet, 0), 2) as pct_diff
from actuals a
join targets t on t.year = a.year

order by validation_name, year;


/* ---------------------------------------------------------
   6) Nonstudent sanity check
   Pass conditions:
   - nonstudent_below <= all_below
   - removed_as_student_heavy >= 0
   --------------------------------------------------------- */
with all_below as (
    select 2019 as year, sum(analysis_weight) as below_alice_all
    from alice_household_final_2019
    where below_alice_flag = 1

    union all

    select 2021 as year, sum(analysis_weight) as below_alice_all
    from alice_household_final_2021
    where below_alice_flag = 1

    union all

    select 2022 as year, sum(analysis_weight) as below_alice_all
    from alice_household_final_2022
    where below_alice_flag = 1

    union all

    select 2023 as year, sum(analysis_weight) as below_alice_all
    from alice_household_final_2023
    where below_alice_flag = 1
),
nonstudent as (
    select 2019 as year, sum(analysis_weight) as below_alice_nonstudent
    from alice_nonstudent_households_2019

    union all

    select 2021 as year, sum(analysis_weight) as below_alice_nonstudent
    from alice_nonstudent_households_2021

    union all

    select 2022 as year, sum(analysis_weight) as below_alice_nonstudent
    from alice_nonstudent_households_2022

    union all

    select 2023 as year, sum(analysis_weight) as below_alice_nonstudent
    from alice_nonstudent_households_2023
)
select
    '08_nonstudent_filter_impact' as validation_name,
    a.year,
    round(a.below_alice_all, 0) as below_alice_all,
    round(n.below_alice_nonstudent, 0) as below_alice_nonstudent,
    round(a.below_alice_all - n.below_alice_nonstudent, 0) as removed_as_student_heavy,
    round(100.0 * (a.below_alice_all - n.below_alice_nonstudent) / nullif(a.below_alice_all, 0), 2) as pct_removed_as_student_heavy
from all_below a
join nonstudent n
  on n.year = a.year
order by year;