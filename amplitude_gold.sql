create or replace table all_events as (

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
        p.element_text,
        p.element_tag,
        p.element_url,
        lead(e.event_time) over (partition by session_id order by event_time) as next_event_time,
        lag(e.event_type) over (partition by session_id order by event_time) as previous_event_type,
        lag(p.page_url) over (partition by session_id order by event_time) as previous_page_url
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
        (case
            when page_url = 'https://www.theinformationlab.co.uk/' then 'The Information Lab - Home'
            else replace(replace(page_url,'https://www.theinformationlab.co.uk/',''),'/',' - ')
        end) as page_title,
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
        end) as click_error
    from join_tables
)

select * from calculations
order by session_id, event_time

);

-- Check for duplicate event_uuids in dim_event_pages
