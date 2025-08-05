--Example:
-- SELECT
--     N."FIRST-NAME",
--     UPPER(LEFT(N."MIDDLE-NAME", 1)) AS "middle-initial",
--     N."LAST-NAME",
--     S."OTHER-ID" AS "student-id",
--     S."STUDENT-ID" AS "internal-id",
--     CASE
--         WHEN N."FEDERAL-ID-NO" != '' THEN N."FEDERAL-ID-NO"
--         ELSE SXT."TEA-STU-ID"
--     END AS "FEDERAL-ID-NO",
--     N."BIRTHDATE" AS "date-of-birth",
--     S."GRADUATION-DATE",
--     SE."ENTITY-ID" AS "grad-school"


-- FROM PUB."STUDENT-ENTITY" SE

-- LEFT JOIN PUB."STUDENT" S
-- ON SE."STUDENT-ID" IN (::at_risk_IDs::)

-- LEFT JOIN PUB."NAME" N
-- ON S."NAME-ID" = N."NAME-ID"

-- LEFT JOIN PUB."STUDENT-EXT" SXT
-- ON SE."STUDENT-ID" = SXT."STUDENT-ID"

-- WHERE SE."X-DEFAULT-ENTITY" = 1
-- AND SE."STUDENT-STATUS" = 'I'
-- AND S."SCHOOL-ID" = ::campus_code::