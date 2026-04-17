USE ROLE IDENTIFIER('<% role %>');
USE WAREHOUSE IDENTIFIER('<% warehouse %>');

-- ================================================================
-- Schema
-- ================================================================
CREATE SCHEMA IF NOT EXISTS <% database %>.ALERTS
    COMMENT = 'DV2 DQ alert objects — notification integration, stored procedures, alerts';

-- ================================================================
-- Secret: Slack webhook path (stored securely, not in plain text)
-- NOTE: rotate this secret after initial testing
-- ================================================================
CREATE OR REPLACE SECRET <% database %>.ALERTS.SLACK_DV_DQ_WEBHOOK_SECRET
    TYPE = GENERIC_STRING
    SECRET_STRING = '<YOUR_SLACK_WEBHOOK_PATH>';

GRANT USAGE ON SECRET <% database %>.ALERTS.SLACK_DV_DQ_WEBHOOK_SECRET TO ROLE ACCOUNTADMIN;

-- ================================================================
-- Notification Integration  (account-level object)
-- ================================================================
CREATE OR REPLACE NOTIFICATION INTEGRATION SLACK_DV_DQ_ALERTS
    ENABLED = TRUE
    TYPE = WEBHOOK
    WEBHOOK_URL = 'https://hooks.slack.com/services/SNOWFLAKE_WEBHOOK_SECRET'
    WEBHOOK_SECRET = <% database %>.ALERTS.SLACK_DV_DQ_WEBHOOK_SECRET
    WEBHOOK_BODY_TEMPLATE = '{"text": "SNOWFLAKE_WEBHOOK_MESSAGE"}'
    WEBHOOK_HEADERS = ('Content-Type'='application/json');

GRANT USAGE ON INTEGRATION SLACK_DV_DQ_ALERTS TO ROLE ACCOUNTADMIN;

-- ================================================================
-- Stored Procedure: Immediate violation alert
-- Fires per violation batch, lists all _ERR failures in last 10 min
-- ================================================================
CREATE OR REPLACE PROCEDURE <% database %>.ALERTS.SP_DQ_VIOLATION_ALERT()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS $$
DECLARE
    msg       VARCHAR DEFAULT '';
    fail_cnt  INTEGER DEFAULT 0;
    c1 CURSOR FOR
        SELECT
            TABLE_DATABASE || '.' || TABLE_SCHEMA || '.' || TABLE_NAME AS table_ref,
            METRIC_NAME,
            EXPECTATION_NAME,
            VALUE
        FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_EXPECTATION_STATUS
        WHERE EXPECTATION_VIOLATED = TRUE
          AND METRIC_NAME LIKE '%_ERR'
          AND SCHEDULED_TIME >= DATEADD(minute, -10, CURRENT_TIMESTAMP())
        ORDER BY TABLE_NAME, METRIC_NAME;
BEGIN
    FOR rec IN c1 DO
        fail_cnt := fail_cnt + 1;
        msg := msg
            || chr(10) || ':x:  *' || rec.table_ref || '*'
            || chr(10) || '    Metric: '      || rec.METRIC_NAME
            || chr(10) || '    Expectation: ' || rec.EXPECTATION_NAME
            || chr(10) || '    Value: *'      || rec.VALUE || '*'
            || chr(10);
    END FOR;

    IF (fail_cnt > 0) THEN
        msg := ':rotating_light: *DV DQ VIOLATION — ' || fail_cnt || ' expectation(s) failed*'
            || chr(10) || msg;
        CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
            SNOWFLAKE.NOTIFICATION.TEXT_PLAIN(
                SNOWFLAKE.NOTIFICATION.SANITIZE_WEBHOOK_CONTENT(
                    REPLACE(:msg, chr(10), chr(92)||'n')
                )
            ),
            SNOWFLAKE.NOTIFICATION.INTEGRATION('SLACK_DV_DQ_ALERTS')
        );
    END IF;

    RETURN msg;
END;
$$;

-- ================================================================
-- Stored Procedure: Daily EOD summary
-- Covers last 24 hours of _ERR DMF runs, 5 PM AEDT daily
-- ================================================================
CREATE OR REPLACE PROCEDURE <% database %>.ALERTS.SP_DQ_DAILY_REPORT()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS $$
DECLARE
    msg       VARCHAR DEFAULT '';
    pass_cnt  INTEGER DEFAULT 0;
    fail_cnt  INTEGER DEFAULT 0;
    failures  VARCHAR DEFAULT '';
    c_fail CURSOR FOR
        SELECT
            TABLE_DATABASE || '.' || TABLE_SCHEMA || '.' || TABLE_NAME AS table_ref,
            METRIC_NAME,
            EXPECTATION_NAME,
            VALUE
        FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_EXPECTATION_STATUS
        WHERE METRIC_NAME LIKE '%_ERR'
          AND SCHEDULED_TIME >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
          AND EXPECTATION_VIOLATED = TRUE
        ORDER BY TABLE_NAME, METRIC_NAME;
BEGIN
    FOR rec IN c_fail DO
        fail_cnt := fail_cnt + 1;
        failures := failures
            || chr(10) || '   :x:  *' || rec.table_ref || '*'
            || ' / ' || rec.METRIC_NAME
            || ' — Value: *' || rec.VALUE || '*';
    END FOR;

    SELECT COUNT(*) INTO :pass_cnt
    FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_EXPECTATION_STATUS
    WHERE METRIC_NAME LIKE '%_ERR'
      AND SCHEDULED_TIME >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
      AND EXPECTATION_VIOLATED = FALSE;

    msg := ':bar_chart: *DV DQ Daily Report — ' || TO_CHAR(CURRENT_DATE(), 'YYYY-MM-DD') || '*'
        || chr(10) || ':white_check_mark:  Passing: *' || pass_cnt || '*'
        || chr(10) || ':x:  Failing: *' || fail_cnt || '*';

    IF (fail_cnt > 0) THEN
        msg := msg || chr(10) || chr(10) || '*Failures:*' || failures;
    ELSE
        msg := msg || chr(10) || chr(10) || ':tada:  All checks passed today.';
    END IF;

    CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
        SNOWFLAKE.NOTIFICATION.TEXT_PLAIN(
            SNOWFLAKE.NOTIFICATION.SANITIZE_WEBHOOK_CONTENT(
                REPLACE(:msg, chr(10), chr(92)||'n')
            )
        ),
        SNOWFLAKE.NOTIFICATION.INTEGRATION('SLACK_DV_DQ_ALERTS')
    );

    RETURN msg;
END;
$$;

-- ================================================================
-- Alert 1: Immediate violation — polls every 5 minutes
-- ================================================================
CREATE OR REPLACE ALERT <% database %>.ALERTS.ALERT_DQ_VIOLATION
    WAREHOUSE = <% warehouse %>
    SCHEDULE = '5 MINUTES'
    IF (EXISTS (
        SELECT 1
        FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_EXPECTATION_STATUS
        WHERE EXPECTATION_VIOLATED = TRUE
          AND METRIC_NAME LIKE '%_ERR'
          AND SCHEDULED_TIME >= DATEADD(minute, -10, CURRENT_TIMESTAMP())
    ))
    THEN CALL <% database %>.ALERTS.SP_DQ_VIOLATION_ALERT();

ALTER ALERT <% database %>.ALERTS.ALERT_DQ_VIOLATION RESUME;

-- ================================================================
-- Alert 2: Daily EOD report — 5 PM AEDT = 06:00 UTC
-- ================================================================
CREATE OR REPLACE ALERT <% database %>.ALERTS.ALERT_DQ_DAILY_REPORT
    WAREHOUSE = <% warehouse %>
    SCHEDULE = 'USING CRON 0 6 * * * UTC'
    IF (EXISTS (SELECT 1))
    THEN CALL <% database %>.ALERTS.SP_DQ_DAILY_REPORT();

ALTER ALERT <% database %>.ALERTS.ALERT_DQ_DAILY_REPORT RESUME;
