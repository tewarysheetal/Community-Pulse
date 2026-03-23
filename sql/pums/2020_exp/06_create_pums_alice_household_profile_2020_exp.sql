DROP TABLE IF EXISTS alice_household_profile_2020_exp;

CREATE TABLE alice_household_profile_2020_exp AS
SELECT
    hb.serialno,
    hb.state,
    hb.puma,
    hb.wgtp,
    hb.np,
    hb.hincp,
    hb.fincp,
    hb.adjinc,
    hb.hht,
    hb.hht2,
    hb.noc,
    hb.npf,
    hb.fparc,
    hsa.person_count,
    hsa.adult_count,
    hsa.college_age_count,
    hsa.student_enrolled_count,
    hsa.likely_college_student_count,
    hsa.employed_count,
    hsa.reference_person_count,
    hsa.reference_person_student_flag,
    hsa.has_any_likely_college_student,
    hsa.has_any_student,
    hsa.has_any_college_age_person,
    hsa.has_any_employed_person,
    hsa.student_share_of_persons,
    hsa.student_share_of_adults,
    hsa.college_age_share_of_persons,
    hsa.student_heavy_flag_rule_a,
    hsa.student_heavy_flag_rule_b
FROM alice_household_base_2020_exp hb
LEFT JOIN alice_household_student_agg_2020_exp hsa
    ON hb.serialno = hsa.serialno;
