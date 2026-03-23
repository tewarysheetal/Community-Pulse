drop table if exists alice_below_alice_stat_profile_2019;

        create table alice_below_alice_stat_profile_2019 as
        with base as (
            select *
            from alice_below_alice_households_2019
        ),
        totals as (
            select
                count(*)::numeric as unweighted_records,
                coalesce(sum(analysis_weight::numeric), 0)::numeric as weighted_households
            from base
        )
        select
            2019::integer as year,
            'complete_below_alice'::text as population_type,
            'overall'::text as metric_group,
            'weighted_households'::text as metric_name,
            round(t.weighted_households, 4)::numeric as metric_value,
            100.0000::numeric as pct_of_households,
            10::integer as sort_order
        from totals t

        union all

        select
            2019,
            'complete_below_alice',
            'overall',
            'unweighted_records',
            t.unweighted_records,
            null::numeric,
            20
        from totals t

        union all

        select
            2019,
            'complete_below_alice',
            'overall',
            'weighted_avg_household_size',
            round(sum(analysis_weight::numeric * coalesce(person_count::numeric, np::numeric, 0)) / nullif(sum(analysis_weight::numeric), 0), 4),
            null::numeric,
            30
        from base

        union all

        select
            2019,
            'complete_below_alice',
            'overall',
            'weighted_avg_adult_count',
            round(sum(analysis_weight::numeric * coalesce(adult_count::numeric, 0)) / nullif(sum(analysis_weight::numeric), 0), 4),
            null::numeric,
            40
        from base

        union all

        select
            2019,
            'complete_below_alice',
            'overall',
            'weighted_avg_child_count',
            round(sum(analysis_weight::numeric * coalesce(noc::numeric, 0)) / nullif(sum(analysis_weight::numeric), 0), 4),
            null::numeric,
            50
        from base

        union all

        select
            2019,
            'complete_below_alice',
            'income',
            'weighted_avg_real_income',
            round(sum(analysis_weight::numeric * coalesce(hincp_adj_real, 0)) / nullif(sum(analysis_weight::numeric), 0), 2),
            null::numeric,
            60
        from base

        union all

        select
            2019,
            'complete_below_alice',
            'income',
            'median_real_income_unweighted',
            round((percentile_cont(0.5) within group (order by hincp_adj_real))::numeric, 2),
            null::numeric,
            70
        from base
        where hincp_adj_real is not null

        union all

        select
            2019,
            'complete_below_alice',
            'income',
            'weighted_avg_annual_alice_threshold',
            round(sum(analysis_weight::numeric * coalesce(annual_alice_threshold, 0)) / nullif(sum(analysis_weight::numeric), 0), 2),
            null::numeric,
            80
        from base

        union all

        select
            2019,
            'complete_below_alice',
            'student_labor',
            'share_student_heavy',
            round(sum(case when coalesce(student_heavy_flag, 0) = 1 then analysis_weight::numeric else 0 end), 4),
            round(100.0 * sum(case when coalesce(student_heavy_flag, 0) = 1 then analysis_weight::numeric else 0 end) / nullif(sum(analysis_weight::numeric), 0), 4),
            90
        from base

        union all

        select
            2019,
            'complete_below_alice',
            'student_labor',
            'share_has_any_student',
            round(sum(case when coalesce(has_any_student, 0) = 1 then analysis_weight::numeric else 0 end), 4),
            round(100.0 * sum(case when coalesce(has_any_student, 0) = 1 then analysis_weight::numeric else 0 end) / nullif(sum(analysis_weight::numeric), 0), 4),
            100
        from base

        union all

        select
            2019,
            'complete_below_alice',
            'student_labor',
            'share_has_any_likely_college_student',
            round(sum(case when coalesce(has_any_likely_college_student, 0) = 1 then analysis_weight::numeric else 0 end), 4),
            round(100.0 * sum(case when coalesce(has_any_likely_college_student, 0) = 1 then analysis_weight::numeric else 0 end) / nullif(sum(analysis_weight::numeric), 0), 4),
            110
        from base

        union all

        select
            2019,
            'complete_below_alice',
            'student_labor',
            'share_reference_person_student',
            round(sum(case when coalesce(reference_person_student_flag, 0) = 1 then analysis_weight::numeric else 0 end), 4),
            round(100.0 * sum(case when coalesce(reference_person_student_flag, 0) = 1 then analysis_weight::numeric else 0 end) / nullif(sum(analysis_weight::numeric), 0), 4),
            120
        from base

        union all

        select
            2019,
            'complete_below_alice',
            'student_labor',
            'share_has_any_employed_person',
            round(sum(case when coalesce(has_any_employed_person, 0) = 1 then analysis_weight::numeric else 0 end), 4),
            round(100.0 * sum(case when coalesce(has_any_employed_person, 0) = 1 then analysis_weight::numeric else 0 end) / nullif(sum(analysis_weight::numeric), 0), 4),
            130
        from base

        union all

        select
            2019,
            'complete_below_alice',
            'composition_hh_comp_key',
            coalesce(hh_comp_key, 'other') as metric_name,
            round(sum(analysis_weight::numeric), 4) as metric_value,
            round(100.0 * sum(analysis_weight::numeric) / nullif(max(t.weighted_households), 0), 4) as pct_of_households,
            200 + case coalesce(hh_comp_key, 'other')
    when '1_adult_0_child' then 1
    when '1_adult_1_child' then 2
    when '1_adult_2plus_child' then 3
    when '2_adult_0_child' then 4
    when '2_adult_1_child' then 5
    when '2_adult_2plus_child' then 6
    when '3plus_adult_0_child' then 7
    when '3plus_adult_1plus_child' then 8
    else 9
end as sort_order
        from base b
        cross join totals t
        group by coalesce(hh_comp_key, 'other')
        order by sort_order, metric_name;
