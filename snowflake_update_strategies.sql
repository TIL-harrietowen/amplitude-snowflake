//INSERT DATA INTO BASE TABLE FROM STREAM
--remeber to replace the from clause to pull from stream
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

//CHECK LOAD
SELECT * FROM amplitude_events_base;

----------------------------------------------------------------------------
//INSERT INTO FACT TABLE FROM BASE
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

//CHECK LOAD
SELECT * FROM fct_all_session_events;

----------------------------------------------------------------------------
//INSERT INTO DIM EVENT PAGES FROM BASE
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

----------------------------------------------------------------------------
//MERGE INTO DIM DEVICES FROM BASE - SCD1
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


----------------------------------------------------------------------------
//INSERT INTO ALL_EVENTS
--filter for new rows happens in the final select due to table calculations requiring the entire dataset to calculate correctly.
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

----------------------------------------------------------------------------
//INSERT INTO SESSION_JOURNEY
--filter for new rows is in the first CTE
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
        GROUP BY e.session_id, d.device_id, d.device_type, d.platform, e.extract_timestamp
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
