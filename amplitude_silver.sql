//BUILD FACT TABLE
---------------------------------------------------------
create or replace table fct_all_session_events as (
    select
        event_uuid,
        session_id,
        device_id,
        event_type,
        event_time,
        extract_timestamp,
        current_timestamp()::timestamp as load_timestamp
    from amplitude_events_base
);

select * from fct_all_session_events;

//BUILD DIM PAGES TABLE
---------------------------------------------------------
create or replace table dim_event_pages as (
    select 
        event_uuid,
        page_url,
        page_counter,
        REGEXP_SUBSTR(page_url, '/([^/]+)/?$', 1, 1, 'e') as page_title,
        REGEXP_SUBSTR(page_url, '/([^/]+)/[^/]+/?$', 1, 1, 'e', 1) as parent_page_title,
        element_text,
        element_tag,
        element_url,
        extract_timestamp,
        current_timestamp()::timestamp as load_timestamp
    from amplitude_events_base
    where page_url is not null
);
select * from dim_event_pages;

//BUILD DIM DEVICES TABLE
---------------------------------------------------------
create or replace table dim_devices as (
    SELECT 
        device_id,
        device_type,
        device_family,
        platform,
        os_version,
        os_name,
        current_timestamp()::timestamp as load_timestamp
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
        qualify row_number() over (partition by device_id order by extract_timestamp desc) = 1
    )
    ORDER BY device_id
);

select * from dim_devices;

