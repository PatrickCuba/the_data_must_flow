-- ASOF Calendar Table
-- Controls PIT/SNOPIT snapshot cadence via flags (daily/weekly/monthly)
-- Rows in this table drive which dates are populated in PIT tables

CREATE OR REPLACE TABLE <database>.<queryassistance_schema>.as_of_date
(
  as_of             DATE        NOT NULL
, year              SMALLINT    NOT NULL
, month             SMALLINT    NOT NULL
, month_name        CHAR(10)
, day_of_month      SMALLINT    NOT NULL
, day_of_week       VARCHAR(9)  NOT NULL
, day_name          CHAR(10)
, week_of_year      SMALLINT    NOT NULL
, day_of_year       SMALLINT    NOT NULL
, month_lastday     SMALLINT    NOT NULL
, week_lastday      SMALLINT    NOT NULL
, week_firstday     SMALLINT    NOT NULL
, date_sid          INT         NOT NULL
)
AS
WITH date_generator AS (
  SELECT DATEADD(DAY, SEQ4(0), '<start_date>') AS as_of
  FROM TABLE(GENERATOR(ROWCOUNT => <rowcount>))
)
SELECT as_of
     , YEAR(as_of)
     , MONTH(as_of)
     , MONTHNAME(as_of)
     , DAY(as_of)
     , DAYOFWEEK(as_of)
     , DAYNAME(as_of)
     , WEEKOFYEAR(as_of)
     , DAYOFYEAR(as_of)
     , CASE WHEN LAST_DAY(as_of) = as_of THEN 1 ELSE 0 END AS month_lastday
     , CASE WHEN LAST_DAY(as_of, 'week') = as_of THEN 1 ELSE 0 END AS week_lastday
     , CASE WHEN DAYNAME(as_of) = 'Mon' THEN 1 ELSE 0 END AS week_firstday
     , YEAR(as_of) * 10000 + MONTH(as_of) * 100 + DAY(as_of) AS date_sid
FROM date_generator
;
