//CREATE SNOWPIPE TO AUTOMATE (STREAM) THE 'COPY INTO' THE RAW TABLE FROM S3
CREATE OR REPLACE PIPE amplitude_events_pipe
    --snowpipe polls the event notifications from the data load metadata
    AUTO_INGEST = TRUE
AS
    --load the s3 bucket data into the empty table
    COPY INTO amplitude_events_raw
    FROM
        (SELECT
            $1,
            METADATA$FILENAME
        FROM @amplitude-stage)
    FILE_FORMAT = (FORMAT_NAME = amplitude-file-format)
    FORCE = FALSE;

//RUN SHOW PIPE TO OBTAIN NOTIFICATION CHANNEL AND ADD TO S3 BUCKET
-- login to aws → go to s3 bucket → properties → event notifications
-- 1. give it a name
-- 2. suffix: .json
-- 3. object creation: all objects
-- 4. select sqs queue
-- 5. paste in notification_channel value
SHOW PIPES;

----------------------------------------------------------------------------
//CREATE STREAM ON EVENTS_RAW
CREATE STREAM amplitude_events_raw_stream
ON TABLE amplitude_events_raw
APPEND_ONLY = TRUE;

//CHECK STREAM
SELECT * FROM amplitude_events_raw_stream;

----------------------------------------------------------------------------
//STORED PROCEDURE RAW TO BASE
CREATE OR REPLACE PROCEDURE sp_events_raw_to_base()
    RETURNS VARCHAR
    LANGUAGE SQL
AS
$$
BEGIN
INSERT INTO amplitude_events_base (
    WITH json_extraction AS (
        SELECT
            json_data:uuid::VARCHAR AS event_uuid,
            json_data:session_id::VARCHAR AS session_id,
            json_data:device_id::VARCHAR AS device_id,
            json_data:device_type::VARCHAR AS device_type,
            json_data:device_family::VARCHAR AS device_family,
            json_data:platform::VARCHAR AS platform,
            json_data:os_version::VARCHAR AS os_version,
            json_data:os_name::VARCHAR AS os_name,
            json_data:event_type::VARCHAR AS event_type,
            json_data:event_time::TIMESTAMP AS event_time,
            json_data:event_properties."[Amplitude] Page URL"::VARCHAR AS page_url,
            json_data:event_properties."[Amplitude] Page Counter"::INT AS page_counter,
            json_data:event_properties."[Amplitude] Element Text"::VARCHAR AS element_text,
            json_data:event_properties."[Amplitude] Element Tag"::VARCHAR AS element_tag,
            json_data:event_properties."[Amplitude] Element Href"::VARCHAR AS element_url,
            TO_TIMESTAMP(REGEXP_SUBSTR(filename, '\\d{4}-\\d{2}-\\d{2}_\\d{1,2}'), 'YYYY-MM-DD_HH24') AS extract_timestamp,
            CURRENT_TIMESTAMP()::TIMESTAMP AS load_timestamp
        FROM amplitude_events_raw_stream
),

    event_type_cleaning AS (
        SELECT
            event_uuid,
            session_id,
            device_id,
            device_type,
            device_family,
            platform,
            os_version,
            os_name,
            INITCAP(CASE
                WHEN CONTAINS(event_type,'[Amplitude]') THEN REPLACE(event_type,'[Amplitude] ','')
                WHEN CONTAINS(event_type,'_') THEN REPLACE(event_type,'_',' ')
                ELSE event_type
            END)::VARCHAR() AS event_type,
            event_time,
            page_url,
            page_counter,
            element_text,
            element_tag,
            element_url,
            extract_timestamp,
            load_timestamp
    FROM json_extraction
    )

    SELECT * FROM event_type_cleaning
);
RETURN 'Amplitude raw to base update completed';

END;
$$;

----------------------------------------------------------------------------
//STORED PROCEDURE BASE TO SILVER
CREATE OR REPLACE PROCEDURE sp_amplitude_silver()
    RETURNS VARCHAR
    LANGUAGE SQL
AS
$$
BEGIN
    //FCT_ALL_SESSION_EVENTS LOAD
    INSERT INTO fct_all_session_events (
        SELECT
            event_uuid,
            session_id,
            device_id,
            event_type,
            event_time,
            extract_timestamp,
            CURRENT_TIMESTAMP()::TIMESTAMP AS load_timestamp
        FROM amplitude_events_base
        WHERE extract_timestamp > (
            SELECT COALESCE(MAX(extract_timestamp), '1900-01-01')
            FROM fct_all_session_events)
    );

    //DIM_EVENT_PAGES LOAD
    INSERT INTO dim_event_pages (
        SELECT
            event_uuid,
            page_url,
            page_counter,
            REGEXP_SUBSTR(page_url, '/([^/]+)/?$', 1, 1, 'e') AS page_title,
            REGEXP_SUBSTR(page_url, '/([^/]+)/[^/]+/?$', 1, 1, 'e', 1) AS parent_page_title,
            element_text,
            element_tag,
            element_url,
            extract_timestamp,
            CURRENT_TIMESTAMP()::TIMESTAMP AS load_timestamp
        FROM amplitude_events_base
        WHERE page_url IS NOT NULL
            AND extract_timestamp > (SELECT MAX(extract_timestamp) FROM dim_event_pages)
    );

    //DIM_DEVICES LOAD
    MERGE INTO dim_devices AS target
    USING
        (SELECT
            device_id,
            device_type,
            device_family,
            platform,
            os_version,
            os_name,
            CURRENT_TIMESTAMP()::TIMESTAMP AS load_timestamp
        FROM (
            SELECT DISTINCT
                device_id,
                device_type,
                device_family,
                platform,
                os_version,
                os_name,
                extract_timestamp
            FROM amplitude_events_base
            QUALIFY ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY extract_timestamp DESC) = 1
        )) AS source
    ON target.device_id = source.device_id
    WHEN MATCHED AND (
        target.device_type IS DISTINCT FROM source.device_type
        OR target.device_family IS DISTINCT FROM source.device_family
        OR target.platform IS DISTINCT FROM source.platform
        OR target.os_version IS DISTINCT FROM source.os_version
        OR target.os_name IS DISTINCT FROM source.os_name
    ) THEN UPDATE SET
            target.device_type = source.device_type,
            target.device_family = source.device_family,
            target.os_version = source.os_version,
            target.os_name = source.os_name
    WHEN NOT MATCHED THEN
        INSERT (device_id,
                device_type,
                device_family,
                platform,
                os_version,
                os_name,
                load_timestamp)
        VALUES (source.device_id,
                source.device_type,
                source.device_family,
                source.platform,
                source.os_version,
                source.os_name,
                CURRENT_TIMESTAMP()::TIMESTAMP)
    ;

RETURN 'Amplitude base to silver update completed';

END;
$$;

----------------------------------------------------------------------------
//STORED PROCEDURE SILVER TO BASE
CREATE OR REPLACE PROCEDURE sp_amplitude_gold()
    RETURNS VARCHAR
    LANGUAGE SQL
AS
$$
BEGIN
    //ALL_EVENTS LOAD
    INSERT INTO all_events (
        WITH join_tables AS (
            SELECT
                e.event_uuid,
                e.session_id,
                e.device_id,
                d.device_type,
                d.platform,
                e.event_type,
                e.event_time,
                p.page_url,
                p.page_counter,
                p.page_title,
                p.parent_page_title,
                p.element_text,
                p.element_tag,
                p.element_url,
                LEAD(e.event_time) OVER (PARTITION BY session_id ORDER BY event_time) AS next_event_time,
                LAG(e.event_type) OVER (PARTITION BY session_id ORDER BY event_time) AS previous_event_type,
                LAG(p.page_url) OVER (PARTITION BY session_id ORDER BY event_time) AS previous_page_url,
                e.extract_timestamp
            FROM fct_all_session_events e
            LEFT JOIN dim_event_pages p
                ON e.event_uuid = p.event_uuid
            LEFT JOIN dim_devices d
                ON e.device_id = d.device_id
        )

        ,calculations AS (
            SELECT
                event_uuid,
                session_id,
                device_id,
                device_type,
                platform,
                event_type,
                event_time,
                (DATEDIFF('second',event_time,next_event_time)*1.0) AS event_duration_s,
                ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time) AS event_counter,
                page_url,
                page_counter,
                page_title,
                parent_page_title,
                element_text,
                element_tag,
                element_url,
                (CASE
                    WHEN event_type = previous_event_type AND page_url = previous_page_url THEN TRUE
                    ELSE FALSE
                END) AS previous_event_repeated,
                (CASE
                    WHEN previous_event_type = 'Element Clicked' AND page_url = previous_page_url THEN TRUE
                    ELSE FALSE
                END) AS click_error,
                extract_timestamp
            FROM join_tables
        )

        , max_ts AS (
            SELECT COALESCE(MAX(extract_timestamp), '1990-01-01'::TIMESTAMP) AS max_extract_timestamp
            FROM all_events
        )

        , filtered AS (
            SELECT c.*
            FROM calculations c, max_ts m
            WHERE c.extract_timestamp > m.max_extract_timestamp
        )

        SELECT *
        FROM filtered
        ORDER BY session_id, event_time
    );

    //SESSION_JOURNEY LOAD
    INSERT INTO session_journey (
        WITH max_ts AS (
            SELECT COALESCE(MAX(extract_timestamp), '1990-01-01'::TIMESTAMP) AS max_extract_timestamp
            FROM session_journey
        )

        ,new_records AS (
            SELECT * FROM fct_all_session_events f, max_ts m
            WHERE f.extract_timestamp > m.max_extract_timestamp
        )

        ,page_view_counts AS (
            SELECT
                e.session_id,
                LISTAGG(p.page_title,', ') AS page_path,
                COUNT(*) AS total_pages_viewed
            FROM new_records e
            LEFT JOIN dim_event_pages p
                ON e.event_uuid = p.event_uuid
            WHERE e.event_type = 'Page Viewed'
            GROUP BY e.session_id
        )

        , all_event_counts AS (
            SELECT
                e.session_id,
                d.device_id,
                d.device_type,
                d.platform,
                COUNT(*) AS total_events,
                MIN(e.event_time) AS session_start_time,
                MAX(e.event_time) AS session_end_time,
                DATEDIFF('second',MIN(e.event_time),MAX(e.event_time)) AS event_duration_s,
                MAX(e.extract_timestamp) AS extract_timestamp
            FROM new_records e
            LEFT JOIN dim_devices d
                ON e.device_id = d.device_id
            GROUP BY e.session_id, d.device_id, d.device_type, d.platform
        )

        ,join_tables AS (
            SELECT
                e.session_id,
                e.device_id,
                e.device_type,
                e.platform,
                p.page_path,
                e.total_events,
                p.total_pages_viewed,
                e.session_start_time,
                e.session_end_time,
                e.event_duration_s,
                e.extract_timestamp
            FROM all_event_counts e
            INNER JOIN page_view_counts p
            ON e.session_id = p.session_id
        )
        SELECT * FROM join_tables
    );


RETURN 'Amplitude Gold Table Updated: events_raw';

END;
$$;


----------------------------------------------------------------------------
//CHECK STORED PROCEDURES WORK
CALL sp_events_raw_to_base();
CALL sp_amplitude_silver();
CALL sp_amplitude_gold();

----------------------------------------------------------------------------
//TASKS

--task: raw to base
CREATE OR REPLACE TASK task_events_raw_to_base
    WAREHOUSE = '<your-warehouse-name>'
    SCHEDULE = 'USING CRON 0 9 * * * Europe/London'
    USER_TASK_TIMEOUT_MS = 60000    --Each task runs after one minute, with a 60-second timeout.
    TASK_AUTO_RETRY_ATTEMPTS = 2    --If a task fails, retry it twice, else entire task graph fails.
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3     --If task graph fails 3 times in a row, suspend the task.
    WHEN SYSTEM$STREAM_HAS_DATA('amplitude_events_raw_stream')
    AS
    CALL sp_events_raw_to_base();

ALTER TASK task_events_raw_to_base RESUME;


--task: base to silver
CREATE OR REPLACE TASK task_events_base_to_silver
WAREHOUSE = '<your-warehouse-name>'
AFTER task_events_raw_to_base
AS
CALL sp_amplitude_silver();

ALTER TASK task_events_base_to_silver RESUME;


--task: silver to gold
CREATE OR REPLACE TASK task_events_silver_to_gold
WAREHOUSE = '<your-warehouse-name>'
AFTER task_events_base_to_silver
AS
CALL sp_amplitude_gold();

ALTER TASK task_events_silver_to_gold RESUME;
