//INSERT DATA INTO BASE TABLE FROM STREAM
--remeber to replace the from clause to pull from stream
INSERT INTO amplitude_events_base (
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
        from amplitude_events_raw_stream
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

//CHECK LOAD
select * from amplitude_events_base;

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
        current_timestamp()::timestamp as load_timestamp
    FROM amplitude_events_base
    WHERE extract_timestamp > (
        SELECT COALESCE(MAX(extract_timestamp), '1900-01-01') 
        FROM fct_all_session_events)
);

//CHECK LOAD
select * from fct_all_session_events;

----------------------------------------------------------------------------
//INSERT INTO DIM EVENT PAGES FROM BASE
INSERT INTO dim_event_pages (
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
        and extract_timestamp > (SELECT MAX(extract_timestamp) FROM dim_event_pages)
);

----------------------------------------------------------------------------
//MERGE INTO DIM DEVICES FROM BASE - SCD1
MERGE INTO dim_devices as target
USING 
    (SELECT 
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
    )) as source
on target.device_id = source.device_id
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
            current_timestamp()::timestamp)
;


----------------------------------------------------------------------------
//INSERT INTO ALL_EVENTS
--filter for new rows happens in the final select due to table calculations requiring the entire dataset to calculate correctly.
INSERT INTO all_events (
    with join_tables as (
        select
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
            lead(e.event_time) over (partition by session_id order by event_time) as next_event_time,
            lag(e.event_type) over (partition by session_id order by event_time) as previous_event_type,
            lag(p.page_url) over (partition by session_id order by event_time) as previous_page_url,
            e.extract_timestamp
        from fct_all_session_events e
        left join dim_event_pages p
            on e.event_uuid = p.event_uuid
        left join dim_devices d
            on e.device_id = d.device_id
    )
    
    ,calculations as (
        select
            event_uuid,
            session_id,
            device_id,
            device_type,
            platform,
            event_type,
            event_time,
            (datediff('second',event_time,next_event_time)*1.0) as event_duration_s,
            row_number() over (partition by session_id order by event_time) as event_counter,
            page_url,
            page_counter,
            page_title,
            parent_page_title,
            element_text,
            element_tag,
            element_url,
            (case 
                when event_type = previous_event_type and page_url = previous_page_url then true
                else false 
            end) as previous_event_repeated,
            (case
                when previous_event_type = 'Element Clicked' and page_url = previous_page_url then true
                else false
            end) as click_error,
            extract_timestamp
        from join_tables
    )
    
    , max_ts as (
        select coalesce(max(extract_timestamp), '1990-01-01'::timestamp) as max_extract_timestamp
        from all_events
    )

    , filtered as (
        select c.*
        from calculations c, max_ts m
        where c.extract_timestamp > m.max_extract_timestamp
    )

    select *
    from filtered
    order by session_id, event_time
);

----------------------------------------------------------------------------
//INSERT INTO SESSION_JOURNEY
--filter for new rows is in the first CTE
INSERT INTO session_journey (
    with new_records as (
        select * from fct_all_session_events
        where extract_timestamp > (select coalesce(max(extract_timestamp), '1900-01-01') from session_journey)
    )
    
    ,page_view_counts as (
        select
            e.session_id,
            listagg(p.page_title,', ') as page_path,
            count(*) as total_pages_viewed
        from new_records e
        left join dim_event_pages p
            on e.event_uuid = p.event_uuid
        where e.event_type = 'Page Viewed'
        group by e.session_id
    )
    
    , all_event_counts as (
        select
            e.session_id,
            d.device_id,
            d.device_type,
            d.platform,
            count(*) as total_events,
            min(e.event_time) as session_start_time,
            max(e.event_time) as session_end_time,
            datediff('second',min(e.event_time),max(e.event_time)) as event_duration_s
        from new_records e
        left join dim_devices d 
            on e.device_id = d.device_id
        group by e.session_id, d.device_id, d.device_type, d.platform, e.extract_timestamp 
    )
    
    ,join_tables as (
        select
            e.session_id,
            e.device_id,
            e.device_type,
            e.platform,
            p.page_path,
            e.total_events,
            p.total_pages_viewed,
            e.session_start_time,
            e.session_end_time,
            e.event_duration_s
        from all_event_counts e
        inner join page_view_counts p 
        on e.session_id = p.session_id
    )
    select * from join_tables
);

