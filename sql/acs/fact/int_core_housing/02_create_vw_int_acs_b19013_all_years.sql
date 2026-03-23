DROP VIEW IF EXISTS public.vw_int_acs_b19013_all_years;

CREATE VIEW public.vw_int_acs_b19013_all_years AS
SELECT * FROM public.int_acs_2019_b19013
UNION ALL
SELECT * FROM public.int_acs_2021_b19013
UNION ALL
SELECT * FROM public.int_acs_2022_b19013
UNION ALL
SELECT * FROM public.int_acs_2023_b19013;
