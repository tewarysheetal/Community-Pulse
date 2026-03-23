DROP VIEW IF EXISTS public.vw_int_acs_dp04_all_years;

CREATE VIEW public.vw_int_acs_dp04_all_years AS
SELECT * FROM public.int_acs_2019_dp04
UNION ALL
SELECT * FROM public.int_acs_2021_dp04
UNION ALL
SELECT * FROM public.int_acs_2022_dp04
UNION ALL
SELECT * FROM public.int_acs_2023_dp04;
