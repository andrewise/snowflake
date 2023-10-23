-- Create a Snowflake Notification Integration
CREATE OR REPLACE NOTIFICATION INTEGRATION notifiction_name
    TYPE=EMAIL
    ENABLED=TRUE
    ALLOWED_RECIPIENTS=('your@email.com');

-- Create a table to register recipients to receive notification emails
CREATE TABLE event_recipients
(
    event_type               STRING,
    notification_integration STRING,
    recipients_email         STRING
);

-- Create a table to register events
CREATE OR REPLACE TABLE event_log
(
    event_id        NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    event_timestamp TIMESTAMP_NTZ,
    event_type      STRING,
    event_message   STRING,
    event_details   VARIANT
);

-- Create alert to check when there is a long-running query that runs for more than the defined timeframe
CREATE OR REPLACE ALERT alert_long_running_queries
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '30 MINUTE' -- Adjust according to your preferences
    IF (EXISTS (
            SELECT 1
            FROM
            TABLE(INFORMATION_SCHEMA.QUERY_HISTORY
            (DATEADD('HOUR',-1,CURRENT_TIMESTAMP()),CURRENT_TIMESTAMP()))
            WHERE execution_status='SUCCESS'
            AND total_elapsed_time>10000
            AND start_time BETWEEN
            IFNULL(SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME(),
            '1900-01-01'::TIMESTAMP_NTZ) AND SNOWFLAKE.ALERT.SCHEDULED_TIME()
        )
    )
    THEN
    INSERT INTO event_log
        (event_timestamp,event_type,event_message,event_details)
    SELECT
        SNOWFLAKE.ALERT.SCHEDULED_TIME(),
        'long_running_query',
        'Long running query ('||query_id||') detected.',
        OBJECT_CONSTRUCT(
        'query_id',query_id,
        'query_type',query_type,
        'start_time',start_time,
        'user_name',user_name,
        'warehouse_name',warehouse_name,
        'end_time',end_time,
        'total_elapsed_time',total_elapsed_time,
        'execution_status',execution_status,
        'error_code',error_code,
        'error_message',error_message
        )
    FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY
    (DATEADD('HOUR',-1,CURRENT_TIMESTAMP()),CURRENT_TIMESTAMP()))
    WHERE execution_status='SUCCESS'
    AND total_elapsed_time>10000
    AND start_time BETWEEN
        IFNULL(SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME(),
        '1900-01-01'::TIMESTAMP_NTZ) AND SNOWFLAKE.ALERT.SCHEDULED_TIME()
    ORDER BY start_time;

-- Create stored procedure in python to send the email
CREATE OR REPLACE PROCEDURE proc_send_email(
    event_recipients VARCHAR,
    event_log VARCHAR,
    timestamp TIMESTAMP_NTZ)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$

import snowflake.snowpark as snowpark

def main(session: snowpark.Session,event_recipients,event_log,timestamp):
    df_email_messages = session.sql("""
            WITH cte_event_recipients AS
            (
                SELECT event_recipients,
                       event_type,
                       notification_integration,
                       LISTAGG(recipients_email,',') WITHIN GROUP (ORDER BY recipients_email)
                FROM {0}
                GROUP BY event_type, notification_integration, event_recipients
            ),
            cte_events(event_type, event_message) AS
            (
                SELECT event_type,
                       'Event ID:'||event_id::STRING||' -> '||event_message
                FROM {1}
                WHERE event_timestamp > '{2}'::DATE
            )
            SELECT er.event_type email_sbject,
                   er.notification_integration,
                   er.recipients_email,
                   LISTAGG(e.event_message||'\n') WITHIN GROUP (ORDER BY e.event_message) email_body
            FROM cte_events e
            INNER JOIN cte_event_recipients er
            ON e.event_type = er.event_type
            GROUP BY 1,2,3
        """.format(event_recipients,event_log,timestamp)).to_pandas()

    for idx,row in df_email_messages.iterrows():
        session.sql("""
        CALL SYSTEM$SEND_EMAIL(
        '{notification_integration}',
        '{recipients_email}',
        '{email_subject}',
        '{email_body}',
        )
        """.format(notification_integration = row["NOTIFICATION_INTEGRATION"],
                       recipients_email = row["RECIPIENTS_EMAIL"]),
                       email_subject = row["EMAIL_SUBJECT"],
                       email_body = row["EMAIL_BODY"]
                       ).collect()
    return "Sent!"
$$;

-- Create the alert that sends the emails
CREATE OR REPLACE ALERT alert_email_events
    WAREHOUSE = compute_wh
    SCHEDULE = '60 minute' -- Adjust according your needs
    IF(EXISTS (
        SELECT *
        FROM EVENT_LOG
        WHERE EVENT_TIMESTAMP BETWEEN IFNULL(SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME(),'1970-01-01'::TIMESTAMP_NTZ)
        AND SNOWFLAKE.ALERT.SCHEDULED_TIME()
        )
    )
    THEN CALL PROC_SEND_EMAIL(
        'EVENT_RECIPIENTS',
        'EVENT_LOG',
        SNOWFLAKE.ALERT.SCHEDULED_TIME()::TIMESTAMP_NTZ);

-- Remember to populate the event recipients table with the data about alerts and recipients
INSERT INTO event_recipients VALUES
('long_running_query', 'your_notification_integration', 'your@email.com');