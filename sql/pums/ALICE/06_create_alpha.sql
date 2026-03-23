drop table if exists alice_puma1902_alpha;
create table alice_puma1902_alpha as
select
    2022 as alloc_year,
    13124::numeric as champaign_households,
    59934::numeric as total_puma1902_households,
    0.218974::numeric as alpha

union all

select
    2023 as alloc_year,
    12903::numeric as champaign_households,
    60149::numeric as total_puma1902_households,
    0.214517::numeric as alpha;