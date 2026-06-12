//BUILD ALL_EVENTS
---------------------------------------------------------
CREATE OR REPLACE TABLE all_events AS (

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

SELECT * FROM calculations
ORDER BY session_id, event_time

);

//BUILD SESSION_JOURNEY
---------------------------------------------------------
CREATE OR REPLACE TABLE session_journey AS (

WITH page_view_counts AS (
    SELECT
        e.session_id,
        LISTAGG(p.page_title,', ') AS page_path,
        COUNT(*) AS total_pages_viewed
    FROM fct_all_session_events e
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
    FROM fct_all_session_events e
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
