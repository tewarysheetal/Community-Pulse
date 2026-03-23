DROP TABLE IF EXISTS alice_household_student_agg_2019;

CREATE TABLE alice_household_student_agg_2019 AS
SELECT
    serialno,
    COUNT(*) AS person_count,
    SUM(is_adult) AS adult_count,
    SUM(is_college_age) AS college_age_count,
    SUM(is_student_enrolled) AS student_enrolled_count,
    SUM(is_likely_college_student) AS likely_college_student_count,
    SUM(is_employed) AS employed_count,
    SUM(is_reference_person) AS reference_person_count,
    MAX(
        CASE
            WHEN is_reference_person = 1 AND is_likely_college_student = 1
            THEN 1 ELSE 0
        END
    ) AS reference_person_student_flag,
    CASE WHEN SUM(is_likely_college_student) > 0 THEN 1 ELSE 0 END AS has_any_likely_college_student,
    CASE WHEN SUM(is_student_enrolled) > 0 THEN 1 ELSE 0 END AS has_any_student,
    CASE WHEN SUM(is_college_age) > 0 THEN 1 ELSE 0 END AS has_any_college_age_person,
    CASE WHEN SUM(is_employed) > 0 THEN 1 ELSE 0 END AS has_any_employed_person,
    SUM(is_likely_college_student)::numeric / NULLIF(COUNT(*), 0) AS student_share_of_persons,
    SUM(is_likely_college_student)::numeric / NULLIF(SUM(is_adult), 0) AS student_share_of_adults,
    SUM(is_college_age)::numeric / NULLIF(COUNT(*), 0) AS college_age_share_of_persons,
    CASE
        WHEN SUM(is_likely_college_student) > 0
         AND (
                MAX(CASE WHEN is_reference_person = 1 AND is_likely_college_student = 1 THEN 1 ELSE 0 END) = 1
                OR
                SUM(is_likely_college_student)::numeric / NULLIF(SUM(is_adult), 0) >= 0.5
             )
        THEN 1 ELSE 0
    END AS student_heavy_flag_rule_a,
    CASE
        WHEN SUM(is_likely_college_student)::numeric / NULLIF(COUNT(*), 0) >= 0.5
         AND SUM(is_employed) = 0
        THEN 1 ELSE 0
    END AS student_heavy_flag_rule_b
FROM alice_person_student_flags_2019
GROUP BY serialno;
