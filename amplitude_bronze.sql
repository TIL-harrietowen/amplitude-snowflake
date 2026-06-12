SELECT * FROM amplitude_events_raw;

//BUILD BASE TABLE ENSURING TO PULL DATA FROM STREAM
---------------------------------------------------------
CREATE OR REPLACE TABLE amplitude_events_base AS (
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
        FROM amplitude_events_raw
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

//CHECK BASE TABLE BUILT SUCCESSFULLY
---------------------------------------------------------
SELECT * FROM amplitude_events_base;
