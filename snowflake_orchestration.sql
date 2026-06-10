//CREATE SNOWPIPE TO AUTOMATE (STREAM) THE 'COPY INTO' THE RAW TABLE FROM S3
CREATE OR REPLACE PIPE <snowpipe-name>
    --SNOWPIPE POLLS THE EVENT NOTIFICATIONS FROM THE DATA LOAD METADATA
    auto_ingest=true
AS
    --load the s3 bucket data into the empty table
    COPY INTO amplitude_events_raw
    FROM
        (SELECT
            $1,
            metadata$filename
        FROM @<stage-name>)
    FILE_FORMAT = (FORMAT_NAME = <file-format-name>)
    FORCE = FALSE;

//RUN SHOW PIPE TO OBTAIN NOTIFICATION CHANNEL AND ADD TO S3 BUCKET
-- login to aws → go to s3 bucket → properties → event notifications
-- 1. give it a name
-- 2. suffix: .json
-- 3. object creation: all objects
-- 4. select sqs queue
-- 5. paste in notification_channel value
show pipes;

----------------------------------------------------------------------------
//CREATE STREAM ON EVENTS_RAW
CREATE STREAM events_raw_to_base_stream
ON TABLE amplitude_events_raw
append_only=true;

//CHECK STREAM
select * from events_raw_to_base_stream;

----------------------------------------------------------------------------
//INSERT DATA INTO BASE TABLE FROM STREAM
--remeber to replace the from clause to pull from stream
INSERT INTO amplitude_events_base (
    with json_extraction as (
        select 
            json_data:uuid::varchar as event_uuid,
            json_data:session_id::varchar as session_id,
            json_data:device_id::varchar as device_id,
            json_data:event_type::varchar as event_type,
            json_data:event_time::timestamp as event_time,
            json_data:event_properties."[Amplitude] Page URL"::varchar as page_url,
            json_data:event_properties."[Amplitude] Page Counter"::int as page_counter,
            json_data:event_properties."[Amplitude] Element Text"::varchar as element_text,
            json_data:event_properties."[Amplitude] Element Tag"::varchar as element_tag,
            json_data:event_properties."[Amplitude] Element Href"::varchar as element_url,
            to_timestamp(REGEXP_SUBSTR(filename, '\\d{4}-\\d{2}-\\d{2}_\\d{1,2}'), 'YYYY-MM-DD_HH24') as load_timestamp
        from EVENTS_RAW_TO_BASE_STREAM
    ),
    
    event_type_cleaning as (
        select
            event_uuid,
            session_id,
            device_id,
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
            load_timestamp
    from json_extraction
    )
    
    select * from event_type_cleaning
);