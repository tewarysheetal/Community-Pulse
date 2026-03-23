DROP VIEW IF EXISTS public.vw_int_acs_b25003_all_years;

CREATE VIEW public.vw_int_acs_b25003_all_years AS
SELECT * FROM public.int_acs_2019_b25003
UNION ALL
SELECT * FROM public.int_acs_2021_b25003
UNION ALL
SELECT * FROM public.int_acs_2022_b25003
UNION ALL
SELECT * FROM public.int_acs_2023_b25003;
