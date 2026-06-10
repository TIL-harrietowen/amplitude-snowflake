SELECT * FROM amplitude_events_raw;

//BUILD BASE TABLE ENSURING TO PULL DATA FROM STREAM
---------------------------------------------------------
create or replace table amplitude_events_base as (
    with json_extraction as (
        select 
            json_data:uuid::varchar as event_uuid,
            json_data:session_id::varchar as session_id,
            json_data:device_id::varchar as device_id,
            json_data:device_type::varchar as device_type,
            json_data:device_family::varchar as device_family,
            json_data:platform::varchar as platform,
            json_data:os_version::varchar as os_version,
            json_data:os_name::varchar as os_name,
            json_data:event_type::varchar as event_type,
            json_data:event_time::timestamp as event_time,
            json_data:event_properties."[Amplitude] Page URL"::varchar as page_url,
            json_data:event_properties."[Amplitude] Page Counter"::int as page_counter,
            json_data:event_properties."[Amplitude] Element Text"::varchar as element_text,
            json_data:event_properties."[Amplitude] Element Tag"::varchar as element_tag,
            json_data:event_properties."[Amplitude] Element Href"::varchar as element_url,
            to_timestamp(REGEXP_SUBSTR(filename, '\\d{4}-\\d{2}-\\d{2}_\\d{1,2}'), 'YYYY-MM-DD_HH24') as extract_timestamp,
            current_timestamp()::timestamp as load_timestamp
        from amplitude_events_raw
),
    
    event_type_cleaning as (
        select
            event_uuid,
            session_id,
            device_id,
            device_type,
            device_family,
            platform,
            os_version,
            os_name,
            INITCAP(case
                when contains(event_type,'[Amplitude]') then replace(event_type,'[Amplitude] ','')
                when contains(event_type,'_') then replace(event_type,'_',' ')
                else event_type
            end)::varchar() as event_type,
            event_time,
            page_url,
            page_counter,
            element_text,
            element_tag,
            element_url,
            extract_timestamp,
            load_timestamp
    from json_extraction
    )
    
    select * from event_type_cleaning
);

//CHECK BASE TABLE BUILT SUCCESSFULLY
---------------------------------------------------------
select * from amplitude_events_base;