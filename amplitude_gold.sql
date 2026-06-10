//BUILD ALL_EVENTS
---------------------------------------------------------
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
        p.page_title,
        p.parent_page_title,
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
        end) as click_error
    from join_tables
)

select * from calculations
order by session_id, event_time

);

//BUILD SESSION_JOURNEY
---------------------------------------------------------
create or replace table session_journey as (

with page_view_counts as (
    select
        e.session_id,
        listagg(p.page_title,', ') as page_path,
        count(*) as total_pages_viewed
    from fct_all_session_events e
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
    from fct_all_session_events e
    left join dim_devices d 
        on e.device_id = d.device_id
    group by e.session_id, d.device_id, d.device_type, d.platform     
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