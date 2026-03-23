DROP VIEW IF EXISTS public.vw_int_acs_b03002_all_years;

CREATE VIEW public.vw_int_acs_b03002_all_years AS
SELECT * FROM public.int_acs_2019_b03002
UNION ALL
SELECT * FROM public.int_acs_2021_b03002
UNION ALL
SELECT * FROM public.int_acs_2022_b03002
UNION ALL
SELECT * FROM public.int_acs_2023_b03002;
