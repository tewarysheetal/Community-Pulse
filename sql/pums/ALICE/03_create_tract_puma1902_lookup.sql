create table tract_puma1902_lookup as
select
    lpad(statefp::text, 2, '0') as statefp,
    lpad(countyfp::text, 3, '0') as countyfp,
    lpad(regexp_replace(tractce::text, '[^0-9]', '', 'g'), 6, '0') as tractce,
    lpad(puma5ce::text, 5, '0') as puma5ce,
    champgf::int as champgf,
    lpad(statefp::text, 2, '0')
      || lpad(countyfp::text, 3, '0')
      || lpad(regexp_replace(tractce::text, '[^0-9]', '', 'g'), 6, '0') as tract_geoid
from tract_il
where lpad(puma5ce::text, 5, '0') = '01902';
