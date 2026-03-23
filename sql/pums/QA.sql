Select '2019' As year, count(*) From household_base_2019
union 
Select '2020' As year, count(*) From household_base_2020_exp
union
Select '2021' As year, count(*) From household_base_2021
union
Select '2022' As year, count(*) From household_base_2022
union
Select '2023' As year, count(*) From household_base_2023;

SELECT '2019' AS yr, COUNT(*) FROM alice_household_profile_2019 WHERE person_count IS NULL
UNION ALL
SELECT '2020_exp', COUNT(*) FROM alice_household_profile_2020_exp WHERE person_count IS NULL
UNION ALL
SELECT '2021', COUNT(*) FROM alice_household_profile_2021 WHERE person_count IS NULL
UNION ALL
SELECT '2022', COUNT(*) FROM alice_household_profile_2022 WHERE person_count IS NULL
UNION ALL
SELECT '2023', COUNT(*) FROM alice_household_profile_2023 WHERE person_count IS NULL;

SELECT year, COUNT(*) AS row_count
FROM alice_household_profile_all_years
GROUP BY year
ORDER BY year;

SELECT
    year,
    COUNT(*) AS sample_households,
    SUM(wgtp) AS weighted_households,
    SUM(CASE WHEN person_count IS NULL THEN 1 ELSE 0 END) AS null_person_rows,
    SUM(CASE WHEN person_count IS NULL THEN wgtp ELSE 0 END) AS weighted_null_person_households
FROM alice_household_profile_all_years
GROUP BY year
ORDER BY year;

SELECT
    year,
    COUNT(*) AS null_person_rows,
    AVG(np) AS avg_np,
    SUM(CASE WHEN np = 0 THEN 1 ELSE 0 END) AS np_zero_count,
    SUM(CASE WHEN hincp IS NULL THEN 1 ELSE 0 END) AS hincp_null_count
FROM alice_household_profile_all_years
WHERE person_count IS NULL
GROUP BY year
ORDER BY year;

SELECT
    year,
    COUNT(*) AS sample_households,
    SUM(wgtp) AS weighted_households
FROM alice_household_profile_all_years_clean
GROUP BY year
ORDER BY year;

SELECT
    year,
    puma,
    COUNT(*) AS sample_households,
    SUM(wgtp) AS weighted_households
FROM alice_household_profile_all_years_clean
WHERE year IN (2022, 2023)
GROUP BY year, puma
ORDER BY year, puma;

select count(*) as total_rows
from tract_puma1902_lookup;

select champgf, count(*) as tract_count
from tract_puma1902_lookup
group by champgf
order by champgf;

select countyfp, count(*) as tract_count
from tract_puma1902_lookup
group by countyfp
order by countyfp;

select count(*) as rows_2022
from acs5_2022_b11001_raw;

select count(*) as rows_2023
from acs5_2023_b11001_raw;

select *
from acs5_2022_b11001_raw
limit 10;

select *
from acs5_2023_b11001_raw
limit 10;

select count(*) as rows_2022
from acs5_2022_puma1902_households;

select count(*) as rows_2023
from acs5_2023_puma1902_households;

select count(*) as missing_2022
from acs5_2022_puma1902_households
where b11001_001e is null;

select count(*) as missing_2023
from acs5_2023_puma1902_households
where b11001_001e is null;

select champgf, count(*) as tract_count, sum(b11001_001e) as households
from acs5_2022_puma1902_households
group by champgf
order by champgf;

select champgf, count(*) as tract_count, sum(b11001_001e) as households
from acs5_2023_puma1902_households
group by champgf
order by champgf;

select *
from puma1902_alpha
order by alloc_year;

select
    2022 as year,
    round(sum(wgtp_adj), 0) as adjusted_households
from alice_household_profile_2022_adj

union all

select
    2023 as year,
    round(sum(wgtp_adj), 0) as adjusted_households
from alice_household_profile_2023_adj;

select
    puma,
    round(sum(wgtp), 0) as raw_wgtp,
    round(sum(wgtp_adj), 0) as adjusted_wgtp,
    round(sum(wgtp_adj) / nullif(sum(wgtp), 0), 6) as adj_ratio
from alice_household_profile_2022_adj
group by puma
order by puma;

select
    puma,
    round(sum(wgtp), 0) as raw_wgtp,
    round(sum(wgtp_adj), 0) as adjusted_wgtp,
    round(sum(wgtp_adj) / nullif(sum(wgtp), 0), 6) as adj_ratio
from alice_household_profile_2023_adj
group by puma
order by puma;

select
    count(*) as rows_all,
    round(sum(wgtp),0) as raw_total,
    round(sum(wgtp_adj),0) as adj_total,

    round(sum(case when coalesce(np,0) > 0 then wgtp else 0 end),0) as raw_np_gt_0,
    round(sum(case when coalesce(np,0) > 0 then wgtp_adj else 0 end),0) as adj_np_gt_0,

    round(sum(case when coalesce(person_count,0) > 0 then wgtp else 0 end),0) as raw_personcount_gt_0,
    round(sum(case when coalesce(person_count,0) > 0 then wgtp_adj else 0 end),0) as adj_personcount_gt_0
from alice_household_profile_2022_adj;


select
    count(*) as rows_all,
    round(sum(wgtp),0) as raw_total,
    round(sum(wgtp_adj),0) as adj_total,

    round(sum(case when coalesce(np,0) > 0 then wgtp else 0 end),0) as raw_np_gt_0,
    round(sum(case when coalesce(np,0) > 0 then wgtp_adj else 0 end),0) as adj_np_gt_0,

    round(sum(case when coalesce(person_count,0) > 0 then wgtp else 0 end),0) as raw_personcount_gt_0,
    round(sum(case when coalesce(person_count,0) > 0 then wgtp_adj else 0 end),0) as adj_personcount_gt_0
from alice_household_profile_2023_adj;

select puma, count(*) as row_count
from alice_household_profile_2022_adj
group by puma
order by puma;

select puma, count(*) as row_count
from alice_household_profile_2023_adj
group by puma
order by puma;

select
    2022 as year,
    round(sum(wgtp_adj), 0) as adjusted_households
from alice_household_profile_2022_adj

union all

select
    2023 as year,
    round(sum(wgtp_adj), 0) as adjusted_households
from alice_household_profile_2023_adj;

select
    2022 as year,
    round(sum(analysis_weight),0) as total_households
from alice_household_final_2022

union all

select
    2023 as year,
    round(sum(analysis_weight),0) as total_households
from alice_household_final_2023;

select column_name
from information_schema.columns
where table_schema = 'public'
  and table_name = 'alice_household_final_2022'
order by ordinal_position;

select
    2019 as year,
    round(sum(analysis_weight), 0) as households
from alice_household_final_2019

union all

select
    2021 as year,
    round(sum(analysis_weight), 0) as households
from alice_household_final_2021

union all

select
    2022 as year,
    round(sum(analysis_weight), 0) as households
from alice_household_final_2022

union all

select
    2023 as year,
    round(sum(analysis_weight), 0) as households
from alice_household_final_2023
order by year;

select *
from alice_thresholds
order by year desc, hh_comp_key;

select
    round(sum(case when below_alice_flag = 1 then analysis_weight else 0 end), 0) as below_alice_households,
    round(sum(case when below_alice_flag = 0 then analysis_weight else 0 end), 0) as above_alice_households,
    round(sum(analysis_weight), 0) as total_households
from alice_household_final_2023;

select
    round(sum(case
        when below_alice_flag = 1 and student_heavy_flag = 0
        then analysis_weight else 0 end), 0) as alice_nonstudent_households
from alice_household_final_2023;

select 2019 as year, round(sum(analysis_weight),0) as alice_nonstudent_households from alice_nonstudent_households_2019
union all
select 2021, round(sum(analysis_weight),0) from alice_nonstudent_households_2021
union all
select 2022, round(sum(analysis_weight),0) from alice_nonstudent_households_2022
union all
select 2023, round(sum(analysis_weight),0) from alice_nonstudent_households_2023
order by year;

select 2019 as year, round(sum(analysis_weight),0) as total_households from alice_household_final_2019
union all
select 2021, round(sum(analysis_weight),0) from alice_household_final_2021
union all
select 2022, round(sum(analysis_weight),0) from alice_household_final_2022
union all
select 2023, round(sum(analysis_weight),0) from alice_household_final_2023
order by year;

select 2019 as year,
       round(sum(case when below_alice_flag = 1 then analysis_weight else 0 end),0) as below_alice_households,
       round(sum(analysis_weight),0) as total_households
from alice_household_final_2019
union all
select 2021,
       round(sum(case when below_alice_flag = 1 then analysis_weight else 0 end),0),
       round(sum(analysis_weight),0)
from alice_household_final_2021
union all
select 2022,
       round(sum(case when below_alice_flag = 1 then analysis_weight else 0 end),0),
       round(sum(analysis_weight),0)
from alice_household_final_2022
union all
select 2023,
       round(sum(case when below_alice_flag = 1 then analysis_weight else 0 end),0),
       round(sum(analysis_weight),0)
from alice_household_final_2023
order by year;

select
    year,
    hh_comp_key,
    monthly_survival_budget,
    annual_alice_threshold
from alice_thresholds
order by year, hh_comp_key;

select hh_comp_key, count(*) as rows, round(sum(analysis_weight),0) as households
from alice_household_final_2019
group by hh_comp_key
order by hh_comp_key;

select hh_comp_key, count(*) as rows, round(sum(analysis_weight),0) as households
from alice_household_final_2021
group by hh_comp_key
order by hh_comp_key;

select hh_comp_key, count(*) as rows, round(sum(analysis_weight),0) as households
from alice_household_final_2022
group by hh_comp_key
order by hh_comp_key;

select distinct hh_comp_key
from alice_thresholds
order by hh_comp_key;

select
    count(*) as total_rows,
    sum(case when hincp_adj_real is null then 1 else 0 end) as null_hincp_adj_real,
    sum(case when hh_comp_key is null then 1 else 0 end) as null_hh_comp_key,
    sum(case when below_alice_flag is null then 1 else 0 end) as null_below_alice_flag
from alice_household_final_2019;

select
    count(*) as total_rows,
    sum(case when hincp_adj_real is null then 1 else 0 end) as null_hincp_adj_real,
    sum(case when hh_comp_key is null then 1 else 0 end) as null_hh_comp_key,
    sum(case when below_alice_flag is null then 1 else 0 end) as null_below_alice_flag
from alice_household_final_2021;

select
    count(*) as total_rows,
    sum(case when hincp_adj_real is null then 1 else 0 end) as null_hincp_adj_real,
    sum(case when hh_comp_key is null then 1 else 0 end) as null_hh_comp_key,
    sum(case when below_alice_flag is null then 1 else 0 end) as null_below_alice_flag
from alice_household_final_2022;

select
    count(*) as total_rows,
    sum(case when hincp_adj_real is null then 1 else 0 end) as null_hincp_adj_real,
    sum(case when hh_comp_key is null then 1 else 0 end) as null_hh_comp_key,
    sum(case when below_alice_flag is null then 1 else 0 end) as null_below_alice_flag
from alice_household_final_2023;

select count(*) as rows_2019 from alice_nonstudent_households_2019;
select count(*) as rows_2021 from alice_nonstudent_households_2021;
select count(*) as rows_2022 from alice_nonstudent_households_2022;
select count(*) as rows_2023 from alice_nonstudent_households_2023;

select
    2019 as year,
    round(sum(case when below_alice_flag = 1 then analysis_weight else 0 end), 0) as stored_flag_weighted,
    round(sum(case when hincp_adj_real < annual_alice_threshold then analysis_weight else 0 end), 0) as direct_compare_weighted
from alice_household_final_2019

union all

select
    2021 as year,
    round(sum(case when below_alice_flag = 1 then analysis_weight else 0 end), 0),
    round(sum(case when hincp_adj_real < annual_alice_threshold then analysis_weight else 0 end), 0)
from alice_household_final_2021

union all

select
    2022 as year,
    round(sum(case when below_alice_flag = 1 then analysis_weight else 0 end), 0),
    round(sum(case when hincp_adj_real < annual_alice_threshold then analysis_weight else 0 end), 0)
from alice_household_final_2022

union all

select
    2023 as year,
    round(sum(case when below_alice_flag = 1 then analysis_weight else 0 end), 0),
    round(sum(case when hincp_adj_real < annual_alice_threshold then analysis_weight else 0 end), 0)
from alice_household_final_2023
order by year;

select
    2019 as year,
    min(hincp_adj_real) as min_income,
    percentile_cont(0.25) within group (order by hincp_adj_real) as p25,
    percentile_cont(0.50) within group (order by hincp_adj_real) as p50,
    percentile_cont(0.75) within group (order by hincp_adj_real) as p75,
    max(hincp_adj_real) as max_income
from alice_household_final_2019

union all

select
    2021 as year,
    min(hincp_adj_real),
    percentile_cont(0.25) within group (order by hincp_adj_real),
    percentile_cont(0.50) within group (order by hincp_adj_real),
    percentile_cont(0.75) within group (order by hincp_adj_real),
    max(hincp_adj_real)
from alice_household_final_2021

union all

select
    2022 as year,
    min(hincp_adj_real),
    percentile_cont(0.25) within group (order by hincp_adj_real),
    percentile_cont(0.50) within group (order by hincp_adj_real),
    percentile_cont(0.75) within group (order by hincp_adj_real),
    max(hincp_adj_real)
from alice_household_final_2022

union all

select
    2023 as year,
    min(hincp_adj_real),
    percentile_cont(0.25) within group (order by hincp_adj_real),
    percentile_cont(0.50) within group (order by hincp_adj_real),
    percentile_cont(0.75) within group (order by hincp_adj_real),
    max(hincp_adj_real)
from alice_household_final_2023
order by year;

select year, hh_comp_key, monthly_survival_budget, annual_alice_threshold
from alice_thresholds
where year in (2019, 2021, 2022, 2023)
order by year, hh_comp_key;

select
    2019 as year,
    round(sum(case when below_alice_flag = 1 then analysis_weight else 0 end), 0) as alice_all,
    round(sum(case when below_alice_flag = 1 and coalesce(student_heavy_flag,0) = 0 then analysis_weight else 0 end), 0) as alice_nonstudent,
    round(sum(case when below_alice_flag = 1 and coalesce(student_heavy_flag,0) = 1 then analysis_weight else 0 end), 0) as alice_student_heavy
from alice_household_final_2019

union all

select
    2021 as year,
    round(sum(case when below_alice_flag = 1 then analysis_weight else 0 end), 0),
    round(sum(case when below_alice_flag = 1 and coalesce(student_heavy_flag,0) = 0 then analysis_weight else 0 end), 0),
    round(sum(case when below_alice_flag = 1 and coalesce(student_heavy_flag,0) = 1 then analysis_weight else 0 end), 0)
from alice_household_final_2021

union all

select
    2022 as year,
    round(sum(case when below_alice_flag = 1 then analysis_weight else 0 end), 0),
    round(sum(case when below_alice_flag = 1 and coalesce(student_heavy_flag,0) = 0 then analysis_weight else 0 end), 0),
    round(sum(case when below_alice_flag = 1 and coalesce(student_heavy_flag,0) = 1 then analysis_weight else 0 end), 0)
from alice_household_final_2022

union all

select
    2023 as year,
    round(sum(case when below_alice_flag = 1 then analysis_weight else 0 end), 0),
    round(sum(case when below_alice_flag = 1 and coalesce(student_heavy_flag,0) = 0 then analysis_weight else 0 end), 0),
    round(sum(case when below_alice_flag = 1 and coalesce(student_heavy_flag,0) = 1 then analysis_weight else 0 end), 0)
from alice_household_final_2023
order by year;

select
    2019 as year,
    round(sum(analysis_weight), 0) as nonstudent_alice_households
from alice_nonstudent_households_2019
union all
select
    2021 as year,
    round(sum(analysis_weight), 0) as nonstudent_alice_households
from alice_nonstudent_households_2021
union all
select
    2022 as year,
    round(sum(analysis_weight), 0) as nonstudent_alice_households
from alice_nonstudent_households_2022
union all
select
    2023 as year,
    round(sum(analysis_weight), 0) as nonstudent_alice_households
from alice_nonstudent_households_2023;

with base as (
    select 2019 as year, hh_comp_key, analysis_weight, hincp_adj_real, annual_alice_threshold
    from alice_household_final_2019
    union all
    select 2021 as year, hh_comp_key, analysis_weight, hincp_adj_real, annual_alice_threshold
    from alice_household_final_2021
    union all
    select 2022 as year, hh_comp_key, analysis_weight, hincp_adj_real, annual_alice_threshold
    from alice_household_final_2022
    union all
    select 2023 as year, hh_comp_key, analysis_weight, hincp_adj_real, annual_alice_threshold
    from alice_household_final_2023
),
test as (
    select
        year,
        analysis_weight,
        hincp_adj_real,
        case
            when hh_comp_key in ('1_adult_0_child', '2_adult_0_child')
                then annual_alice_threshold + 7500
            else annual_alice_threshold
        end as threshold_test
    from base
)
select
    year,
    round(sum(case when hincp_adj_real < threshold_test then analysis_weight else 0 end), 0) as below_alice_test
from test
group by year
order by year;

with base as (
    select 2019 as year, hh_comp_key, analysis_weight, hincp_adj_real, annual_alice_threshold
    from alice_household_final_2019
    union all
    select 2021 as year, hh_comp_key, analysis_weight, hincp_adj_real, annual_alice_threshold
    from alice_household_final_2021
    union all
    select 2022 as year, hh_comp_key, analysis_weight, hincp_adj_real, annual_alice_threshold
    from alice_household_final_2022
    union all
    select 2023 as year, hh_comp_key, analysis_weight, hincp_adj_real, annual_alice_threshold
    from alice_household_final_2023
),
test as (
    select
        year,
        analysis_weight,
        hincp_adj_real,
        case
            when year in (2019, 2021, 2022)
                 and hh_comp_key in ('1_adult_0_child', '2_adult_0_child')
                then annual_alice_threshold + 5000
            else annual_alice_threshold
        end as threshold_test
    from base
)
select
    year,
    round(sum(case when hincp_adj_real < threshold_test then analysis_weight else 0 end), 0) as below_alice_test
from test
group by year
order by year;

with base as (
    select 2019 as year, hh_comp_key, analysis_weight, hincp_adj_real, annual_alice_threshold
    from alice_household_final_2019
    union all
    select 2021 as year, hh_comp_key, analysis_weight, hincp_adj_real, annual_alice_threshold
    from alice_household_final_2021
    union all
    select 2022 as year, hh_comp_key, analysis_weight, hincp_adj_real, annual_alice_threshold
    from alice_household_final_2022
    union all
    select 2023 as year, hh_comp_key, analysis_weight, hincp_adj_real, annual_alice_threshold
    from alice_household_final_2023
),
test as (
    select
        year,
        analysis_weight,
        hincp_adj_real,
        case
            when year in (2019, 2022)
                 and hh_comp_key in ('1_adult_0_child', '2_adult_0_child')
                then annual_alice_threshold + 5000
            when year = 2021
                 and hh_comp_key in ('1_adult_0_child', '2_adult_0_child')
                then annual_alice_threshold + 7500
            else annual_alice_threshold
        end as threshold_test
    from base
)
select
    year,
    round(sum(case when hincp_adj_real < threshold_test then analysis_weight else 0 end), 0) as below_alice_test
from test
group by year
order by year;

with base as (
    select 2019 as year, hh_comp_key, analysis_weight, hincp_adj_real, annual_alice_threshold
    from alice_household_final_2019
    union all
    select 2021 as year, hh_comp_key, analysis_weight, hincp_adj_real, annual_alice_threshold
    from alice_household_final_2021
    union all
    select 2022 as year, hh_comp_key, analysis_weight, hincp_adj_real, annual_alice_threshold
    from alice_household_final_2022
    union all
    select 2023 as year, hh_comp_key, analysis_weight, hincp_adj_real, annual_alice_threshold
    from alice_household_final_2023
),
test as (
    select
        year,
        analysis_weight,
        hincp_adj_real,
        case
            when year = 2019
                 and hh_comp_key in ('1_adult_0_child', '2_adult_0_child')
                then annual_alice_threshold + 5000
            when year = 2021
                 and hh_comp_key in ('1_adult_0_child', '2_adult_0_child')
                then annual_alice_threshold + 7500
            when year = 2022
                 and hh_comp_key in ('1_adult_0_child', '2_adult_0_child')
                then annual_alice_threshold + 7500
            else annual_alice_threshold
        end as threshold_test
    from base
)
select
    year,
    round(sum(case when hincp_adj_real < threshold_test then analysis_weight else 0 end), 0) as below_alice_test
from test
group by year
order by year;

