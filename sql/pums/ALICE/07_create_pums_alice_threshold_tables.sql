drop table if exists alice_illinois_essentials_index;

create table alice_illinois_essentials_index (
    year integer primary key,
    alice_essentials_index numeric,
    ratio_to_2023 numeric
);

insert into alice_illinois_essentials_index (year, alice_essentials_index)
values
    (2019, 275),
(2021, 286),
(2022, 304),
(2023, 318);

update alice_illinois_essentials_index i
set ratio_to_2023 =
    i.alice_essentials_index / b.alice_essentials_index
from (
    select alice_essentials_index
    from alice_illinois_essentials_index
    where year = 2023
) b;

create table if not exists alice_thresholds (
    year integer not null,
    hh_comp_key text not null,
    adults_18_64 integer not null,
    adults_65_plus integer not null default 0,
    infants_0_2 integer not null default 0,
    preschoolers_3_4 integer not null default 0,
    school_age_5_17 integer not null default 0,
    monthly_survival_budget numeric,
    annual_survival_budget numeric,
    annual_alice_threshold numeric,
    threshold_source text,
    notes text,
    primary key (year, hh_comp_key)
);

delete from alice_thresholds;

insert into alice_thresholds (
    year,
    hh_comp_key,
    adults_18_64,
    adults_65_plus,
    infants_0_2,
    preschoolers_3_4,
    school_age_5_17,
    threshold_source,
    notes
)
values
    (2019, '1_adult_0_child', 1, 0, 0, 0, 0,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'Single adult'),
(2019, '1_adult_1_child', 1, 0, 0, 0, 1,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', '1 adult + 1 school-age child'),
(2019, '1_adult_2plus_child', 1, 0, 0, 0, 2,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'v1 uses 2 school-age children'),
(2019, '2_adult_0_child', 2, 0, 0, 0, 0,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'Two adults'),
(2019, '2_adult_1_child', 2, 0, 0, 0, 1,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', '2 adults + 1 school-age child'),
(2019, '2_adult_2plus_child', 2, 0, 0, 0, 2,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'v1 uses 2 school-age children'),
(2019, '3plus_adult_0_child', 3, 0, 0, 0, 0,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'v1 uses 3 adults'),
(2019, '3plus_adult_1plus_child', 3, 0, 0, 0, 1,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'v1 uses 3 adults + 1 school-age child'),
(2019, 'other', 1, 0, 0, 0, 0,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'review manually'),
(2021, '1_adult_0_child', 1, 0, 0, 0, 0,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'Single adult'),
(2021, '1_adult_1_child', 1, 0, 0, 0, 1,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', '1 adult + 1 school-age child'),
(2021, '1_adult_2plus_child', 1, 0, 0, 0, 2,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'v1 uses 2 school-age children'),
(2021, '2_adult_0_child', 2, 0, 0, 0, 0,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'Two adults'),
(2021, '2_adult_1_child', 2, 0, 0, 0, 1,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', '2 adults + 1 school-age child'),
(2021, '2_adult_2plus_child', 2, 0, 0, 0, 2,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'v1 uses 2 school-age children'),
(2021, '3plus_adult_0_child', 3, 0, 0, 0, 0,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'v1 uses 3 adults'),
(2021, '3plus_adult_1plus_child', 3, 0, 0, 0, 1,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'v1 uses 3 adults + 1 school-age child'),
(2021, 'other', 1, 0, 0, 0, 0,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'review manually'),
(2022, '1_adult_0_child', 1, 0, 0, 0, 0,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'Single adult'),
(2022, '1_adult_1_child', 1, 0, 0, 0, 1,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', '1 adult + 1 school-age child'),
(2022, '1_adult_2plus_child', 1, 0, 0, 0, 2,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'v1 uses 2 school-age children'),
(2022, '2_adult_0_child', 2, 0, 0, 0, 0,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'Two adults'),
(2022, '2_adult_1_child', 2, 0, 0, 0, 1,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', '2 adults + 1 school-age child'),
(2022, '2_adult_2plus_child', 2, 0, 0, 0, 2,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'v1 uses 2 school-age children'),
(2022, '3plus_adult_0_child', 3, 0, 0, 0, 0,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'v1 uses 3 adults'),
(2022, '3plus_adult_1plus_child', 3, 0, 0, 0, 1,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'v1 uses 3 adults + 1 school-age child'),
(2022, 'other', 1, 0, 0, 0, 0,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'review manually'),
(2023, '1_adult_0_child', 1, 0, 0, 0, 0,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'Single adult'),
(2023, '1_adult_1_child', 1, 0, 0, 0, 1,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', '1 adult + 1 school-age child'),
(2023, '1_adult_2plus_child', 1, 0, 0, 0, 2,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'v1 uses 2 school-age children'),
(2023, '2_adult_0_child', 2, 0, 0, 0, 0,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'Two adults'),
(2023, '2_adult_1_child', 2, 0, 0, 0, 1,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', '2 adults + 1 school-age child'),
(2023, '2_adult_2plus_child', 2, 0, 0, 0, 2,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'v1 uses 2 school-age children'),
(2023, '3plus_adult_0_child', 3, 0, 0, 0, 0,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'v1 uses 3 adults'),
(2023, '3plus_adult_1plus_child', 3, 0, 0, 0, 1,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'v1 uses 3 adults + 1 school-age child'),
(2023, 'other', 1, 0, 0, 0, 0,
 'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', 'review manually');
