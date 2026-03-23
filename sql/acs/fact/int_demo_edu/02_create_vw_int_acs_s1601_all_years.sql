DROP VIEW IF EXISTS public.vw_int_acs_s1601_all_years;

CREATE VIEW public.vw_int_acs_s1601_all_years AS
SELECT * FROM public.int_acs_2019_s1601
UNION ALL
SELECT * FROM public.int_acs_2021_s1601
UNION ALL
SELECT * FROM public.int_acs_2022_s1601
UNION ALL
SELECT * FROM public.int_acs_2023_s1601;
