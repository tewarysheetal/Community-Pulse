CREATE TABLE person_student_flags_2023 AS
SELECT
    serialno,
    sporder,
    agep,
    relshipp,
    sch,
    schg,
    schl,
    esr,
    CASE WHEN agep >= 18 THEN 1 ELSE 0 END AS is_adult,
    CASE WHEN agep BETWEEN 18 AND 24 THEN 1 ELSE 0 END AS is_college_age,
    CASE WHEN sch IN (2, 3) THEN 1 ELSE 0 END AS is_student_enrolled,
    CASE
        WHEN agep BETWEEN 18 AND 24
         AND sch IN (2, 3)
         AND schg IN (15, 16)
        THEN 1 ELSE 0
    END AS is_likely_college_student,
    CASE WHEN relshipp = 20 THEN 1 ELSE 0 END AS is_reference_person,
    CASE WHEN esr IN (1, 2) THEN 1 ELSE 0 END AS is_employed
FROM person_2023_champaign;
