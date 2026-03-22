create table acs5_2023_puma1902_households as
select
    l.statefp,
    l.countyfp,
    l.tractce,
    l.puma5ce,
    l.champgf,
    l.tract_geoid,
    a."NAME",
    a.b11001_001e
from tract_puma1902_lookup l
left join acs5_2023_b11001_raw a
    on l.tract_geoid = a.tract_geoid;