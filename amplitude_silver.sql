//BUILD FACT TABLE
---------------------------------------------------------
CREATE OR REPLACE TABLE fct_all_session_events AS (
    SELECT
        event_uuid,
        session_id,
        device_id,
        event_type,
        event_time,
        extract_timestamp,
        CURRENT_TIMESTAMP()::TIMESTAMP AS load_timestamp
    FROM amplitude_events_base
);

SELECT * FROM fct_all_session_events;

//BUILD DIM PAGES TABLE
---------------------------------------------------------
CREATE OR REPLACE TABLE dim_event_pages AS (
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
);
SELECT * FROM dim_event_pages;

//BUILD DIM DEVICES TABLE
---------------------------------------------------------
CREATE OR REPLACE TABLE dim_devices AS (
    SELECT
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
    )
    ORDER BY device_id
);

SELECT * FROM dim_devices;
