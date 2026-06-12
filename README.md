# Amplitude Snowflake

A set of Snowflake SQL scripts that load raw Amplitude event data from AWS S3 and transform it through a medallion architecture (raw → bronze → silver → gold), orchestrated via Snowflake Streams, Stored Procedures, and Tasks.

This repo is part of a wider pipeline:

| Stage                       | Repo                                                                                          |
| --------------------------- | --------------------------------------------------------------------------------------------- |
| Extract + S3 load           | [amplitude-python-extraction](https://github.com/TIL-harrietowen/amplitude-python-extraction) |
| Snowflake load + transform  | **This repo**                                                                                 |
| dbt transform (alternative) | [amplitude-dbt](https://github.com/TIL-harrietowen/amplitude-dbt)                             |

---

## Architecture

```
S3 (JSON files)
    │   Snowpipe (auto-ingest on file arrival)
    ▼
amplitude_events_raw              ← raw:    full JSON + filename stored as VARIANT
    │   Stream (append-only) → task_events_raw_to_base → sp_events_raw_to_base()
    ▼
amplitude_events_base             ← bronze: typed fields extracted, event types cleaned
    │   task_events_base_to_silver → sp_amplitude_silver()
    ▼
fct_all_session_events            ← silver: core event fact table
dim_event_pages                   ← silver: page and element attributes per event
dim_devices                       ← silver: one row per device_id (SCD Type 1 via MERGE)
    │   task_events_silver_to_gold → sp_amplitude_gold()
    ▼
all_events                        ← gold: fully joined, enriched event-level table
session_journey                   ← gold: session-level aggregation
```

---

## File structure

```
amplitude-snowflake/
├── s3_to_snowflake_load.sql        # One-time setup: storage integration, external stage, raw table, COPY INTO, Snowpipe
├── amplitude_bronze.sql            # Initial build: amplitude_events_base
├── amplitude_silver.sql            # Initial build: fct_all_session_events, dim_event_pages, dim_devices
├── amplitude_gold.sql              # Initial build: all_events, session_journey
├── snowflake_orchestration.sql     # Ongoing loads: Stream, stored procedures, and Tasks
└── snowflake_update_strategies.sql # Reference: standalone SQL behind each stored procedure (development use only)
```

---

## Prerequisites

- An AWS S3 bucket containing Amplitude JSON files (see [amplitude-python-extraction](https://github.com/TIL-harrietowen/amplitude-python-extraction))
- An AWS IAM Role with read access to the S3 bucket
- A Snowflake account with `ACCOUNTADMIN` or `SYSADMIN` privileges to create storage integrations and tasks

---

## Configuration

Before running the scripts, replace the following placeholders with your own values:

| Placeholder                    | Where                         | What to put                           |
| ------------------------------ | ----------------------------- | ------------------------------------- |
| `<aws-role-arn>`               | `s3_to_snowflake_load.sql`    | ARN of your AWS IAM Role              |
| `s3://my-bucket/path/to/data/` | `s3_to_snowflake_load.sql`    | Your S3 bucket path                   |
| `<your-warehouse-name>`        | `snowflake_orchestration.sql` | Your Snowflake virtual warehouse name |

---

## Running order

Scripts are run in two phases: an initial one-time build, then ongoing orchestration for new data.

### Phase 1 — one-time setup and initial build

```
1. s3_to_snowflake_load.sql     — AWS + Snowflake connection, raw table, initial COPY INTO
2. amplitude_bronze.sql         — builds amplitude_events_base
3. amplitude_silver.sql         — builds fct_all_session_events, dim_event_pages, dim_devices
4. amplitude_gold.sql           — builds all_events, session_journey
5. snowflake_orchestration.sql  — creates Stream, Snowpipe, stored procedures, and Tasks
```

### Phase 2 — ongoing daily loads

Once the orchestration objects are in place, new data flows automatically:

```
S3 (new JSON files arrive)
    → Snowpipe loads into amplitude_events_raw
    → Stream captures new rows
    → task_events_raw_to_base fires (when stream has data, daily at 09:00 Europe/London)
        → calls sp_events_raw_to_base()
    → task_events_base_to_silver fires (after raw to base completes)
        → calls sp_amplitude_silver()
    → task_events_silver_to_gold fires (after base to silver completes)
        → calls sp_amplitude_gold()
```

The tasks are chained — each only fires after the previous one completes successfully. If a task fails it retries twice before the task graph suspends after 3 consecutive failures.

---

## Layer by layer

### Raw — `s3_to_snowflake_load.sql`

Connects Snowflake to S3 and loads raw JSON into a staging table.

> 💡 **IAM Role vs IAM User** — the Python extraction pipeline uses an IAM User (key + secret). Snowflake uses an IAM Role instead. Access is granted via a trust relationship — no long-lived credentials are stored in Snowflake.

**Setup steps:**

1. Create the storage integration, passing your AWS Role ARN
2. Run `DESC INTEGRATION` to retrieve `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`
3. Add these values to the AWS IAM Role's trust policy to complete the two-way handshake
4. Create the file format, external stage, and raw table
5. Run `LIST @amplitude-stage` to verify the connection
6. Run `COPY INTO` once manually to load all existing files
7. Create the Snowpipe for ongoing automated loads

**Objects created:**

| Object                     | Type                | Purpose                                                                |
| -------------------------- | ------------------- | ---------------------------------------------------------------------- |
| `amplitude-schema`         | Schema              | Dedicated schema for all Amplitude objects                             |
| `amplitude-s3-integration` | Storage Integration | Authorisation between Snowflake and S3                                 |
| `amplitude-file-format`    | File Format         | JSON parser (`STRIP_OUTER_ARRAY = FALSE` — Amplitude files are NDJSON) |
| `amplitude-stage`          | External Stage      | Live pointer to the S3 path — does not store data                      |
| `amplitude_events_raw`     | Table               | Raw JSON + filename, one row per event                                 |
| `amplitude_events_pipe`    | Snowpipe            | Auto-ingests new JSON files from S3 on arrival                         |

**`amplitude_events_raw` schema:**

| Column      | Type    | Notes                                                                             |
| ----------- | ------- | --------------------------------------------------------------------------------- |
| `json_data` | VARIANT | Full raw JSON event object                                                        |
| `filename`  | VARCHAR | Source filename — encodes the extraction hour, used as incremental key downstream |

> ⚠️ **Snowpipe vs Snowflake Task:** Snowpipe triggers on every file arrival. Since Amplitude data only needs loading once per day, a Snowflake Task on a fixed schedule would be more cost-efficient. Snowpipe is implemented here as a course exercise.

---

### Bronze — `amplitude_bronze.sql`

Extracts typed fields from the raw JSON and applies light cleaning. No business logic at this layer.

**Object created:** `amplitude_events_base`

**What it does:**

- Extracts fields from `json_data` using Snowflake colon notation (e.g. `json_data:event_type::VARCHAR`)
- Extracts nested `event_properties` using dot notation with quoted Amplitude field names
- Parses `extract_timestamp` from the filename using `REGEXP_SUBSTR` — the source-aligned incremental key used throughout all downstream layers
- Cleans `event_type` values: strips `[Amplitude]` prefixes, replaces underscores with spaces, applies `INITCAP`
- Adds `load_timestamp` via `CURRENT_TIMESTAMP()` to record when the row entered Snowflake

**Fields:**

| Field               | Source                                                  |
| ------------------- | ------------------------------------------------------- |
| `event_uuid`        | `json_data:uuid`                                        |
| `session_id`        | `json_data:session_id`                                  |
| `device_id`         | `json_data:device_id`                                   |
| `device_type`       | `json_data:device_type`                                 |
| `device_family`     | `json_data:device_family`                               |
| `platform`          | `json_data:platform`                                    |
| `os_version`        | `json_data:os_version`                                  |
| `os_name`           | `json_data:os_name`                                     |
| `event_type`        | `json_data:event_type` (cleaned)                        |
| `event_time`        | `json_data:event_time`                                  |
| `page_url`          | `json_data:event_properties."[Amplitude] Page URL"`     |
| `page_counter`      | `json_data:event_properties."[Amplitude] Page Counter"` |
| `element_text`      | `json_data:event_properties."[Amplitude] Element Text"` |
| `element_tag`       | `json_data:event_properties."[Amplitude] Element Tag"`  |
| `element_url`       | `json_data:event_properties."[Amplitude] Element Href"` |
| `extract_timestamp` | Parsed from `filename` via `REGEXP_SUBSTR`              |
| `load_timestamp`    | `CURRENT_TIMESTAMP()`                                   |

---

### Silver — `amplitude_silver.sql`

Splits the bronze table into a fact table and two dimension tables following a star schema pattern. Used for the initial full build — ongoing loads are handled by `sp_amplitude_silver()`.

**Objects created:**

**`fct_all_session_events`** — one row per event

| Field               | Description                    |
| ------------------- | ------------------------------ |
| `event_uuid`        | Primary key                    |
| `session_id`        | Session the event belongs to   |
| `device_id`         | Foreign key to `dim_devices`   |
| `event_type`        | Cleaned event type             |
| `event_time`        | Timestamp of the event         |
| `extract_timestamp` | Source-aligned incremental key |
| `load_timestamp`    | When the row was loaded        |

**`dim_event_pages`** — page and element context per event (rows where `page_url IS NOT NULL`)

| Field               | Description                                               |
| ------------------- | --------------------------------------------------------- |
| `event_uuid`        | Foreign key to `fct_all_session_events`                   |
| `page_url`          | Full page URL                                             |
| `page_counter`      | Page view counter within the session                      |
| `page_title`        | Last URL segment, extracted via `REGEXP_SUBSTR`           |
| `parent_page_title` | Second-to-last URL segment, extracted via `REGEXP_SUBSTR` |
| `element_text`      | Text of the interacted element                            |
| `element_tag`       | HTML tag of the element                                   |
| `element_url`       | Href of the element                                       |
| `extract_timestamp` | Source-aligned incremental key                            |
| `load_timestamp`    | When the row was loaded                                   |

**`dim_devices`** — one row per `device_id`, most recent record kept via `QUALIFY ROW_NUMBER()`

| Field            | Description              |
| ---------------- | ------------------------ |
| `device_id`      | Primary key              |
| `device_type`    | e.g. Desktop, Mobile     |
| `device_family`  | e.g. Mac, iPhone         |
| `platform`       | e.g. Web                 |
| `os_version`     | Operating system version |
| `os_name`        | Operating system name    |
| `load_timestamp` | When the row was loaded  |

---

### Gold — `amplitude_gold.sql`

Joins and enriches the silver tables into analysis-ready output tables. Used for the initial full build — ongoing loads are handled by `sp_amplitude_gold()`.

**Objects created:**

**`all_events`** — fully joined, enriched event-level table

Joins `fct_all_session_events`, `dim_event_pages`, and `dim_devices`. Adds:

| Field                     | Description                                                                                                       |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `event_duration_s`        | Seconds between this event and the next (`LEAD` window function)                                                  |
| `event_counter`           | Sequential event number within the session (`ROW_NUMBER`)                                                         |
| `previous_event_type`     | The preceding event type in the session (`LAG`)                                                                   |
| `previous_event_repeated` | True if the same event type occurred on the same page as the previous event                                       |
| `click_error`             | True if an Element Clicked event was preceded by another on the same page — potential rage click or error pattern |

**`session_journey`** — one row per session

| Field                | Description                                     |
| -------------------- | ----------------------------------------------- |
| `session_id`         | Primary key                                     |
| `device_id`          | Device used in the session                      |
| `device_type`        | Device type                                     |
| `platform`           | Platform                                        |
| `page_path`          | Ordered list of page titles visited (`LISTAGG`) |
| `total_events`       | Total number of events in the session           |
| `total_pages_viewed` | Count of Page Viewed events                     |
| `session_start_time` | First event timestamp                           |
| `session_end_time`   | Last event timestamp                            |
| `event_duration_s`   | Total session duration in seconds               |
| `extract_timestamp`  | Source-aligned incremental key                  |

---

### Orchestration — `snowflake_orchestration.sql`

Sets up all objects required for ongoing incremental loads after the initial build.

**Objects created:**

| Object                        | Type                          | Purpose                                                                                                      |
| ----------------------------- | ----------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `amplitude_events_raw_stream` | Stream (`APPEND_ONLY = TRUE`) | Captures new rows loaded into `amplitude_events_raw` by Snowpipe                                             |
| `sp_events_raw_to_base()`     | Stored Procedure              | Reads from stream, applies JSON extraction and cleaning, inserts into `amplitude_events_base`                |
| `sp_amplitude_silver()`       | Stored Procedure              | Inserts new rows into `fct_all_session_events` and `dim_event_pages`; merges into `dim_devices` (SCD Type 1) |
| `sp_amplitude_gold()`         | Stored Procedure              | Inserts new rows into `all_events` and `session_journey`                                                     |
| `task_events_raw_to_base`     | Task                          | Triggers `sp_events_raw_to_base()` daily at 09:00 Europe/London when the stream has data                     |
| `task_events_base_to_silver`  | Task                          | Triggers `sp_amplitude_silver()` after `task_events_raw_to_base` completes                                   |
| `task_events_silver_to_gold`  | Task                          | Triggers `sp_amplitude_gold()` after `task_events_base_to_silver` completes                                  |

**Task configuration:**

- Root task (`task_events_raw_to_base`) runs on a cron schedule: `0 9 * * * Europe/London`
- `WHEN SYSTEM$STREAM_HAS_DATA('amplitude_events_raw_stream')` — only fires if new data has arrived; skips silently if the stream is empty
- `USER_TASK_TIMEOUT_MS = 60000` — each task times out after 60 seconds
- `TASK_AUTO_RETRY_ATTEMPTS = 2` — failed tasks retry twice before the graph fails
- `SUSPEND_TASK_AFTER_NUM_FAILURES = 3` — entire task graph suspends after 3 consecutive failures

**Incremental load strategies by table:**

| Table                    | Strategy                                    | Why                                                                                                                           |
| ------------------------ | ------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `amplitude_events_base`  | Stream + INSERT                             | Stream ensures only new raw rows are processed — consumed on each run                                                         |
| `fct_all_session_events` | INSERT with `MAX(extract_timestamp)` filter | Append-only — events never change                                                                                             |
| `dim_event_pages`        | INSERT with `MAX(extract_timestamp)` filter | Append-only — page events never change                                                                                        |
| `dim_devices`            | MERGE (SCD Type 1)                          | Device attributes can change — new records inserted, changed records updated, `IS DISTINCT FROM` prevents unnecessary updates |
| `all_events`             | INSERT filtered after window calculations   | `LEAD`/`LAG` require full session context — filtering earlier would produce incorrect values                                  |
| `session_journey`        | INSERT with `MAX(extract_timestamp)` filter | New sessions only — existing sessions are not reopened                                                                        |

**To run the stored procedures manually:**

```sql
CALL sp_events_raw_to_base();
CALL sp_amplitude_silver();
CALL sp_amplitude_gold();
```

**To suspend or resume tasks:**

```sql
-- Suspend
ALTER TASK task_events_raw_to_base SUSPEND;
ALTER TASK task_events_base_to_silver SUSPEND;
ALTER TASK task_events_silver_to_gold SUSPEND;

-- Resume
ALTER TASK task_events_raw_to_base RESUME;
ALTER TASK task_events_base_to_silver RESUME;
ALTER TASK task_events_silver_to_gold RESUME;
```

> ⚠️ Child tasks (`task_events_base_to_silver`, `task_events_silver_to_gold`) must be resumed before the root task, otherwise Snowflake will reject the root task resume.

---

### Reference — `snowflake_update_strategies.sql`

Contains the raw SQL for each incremental update step as standalone statements. Used during development to test and iterate on the logic before wrapping into stored procedures. Not part of the regular pipeline — do not run in sequence.

---

## Key design decisions

- **`VARIANT` for raw JSON** — the schema never needs to change if Amplitude adds new event properties; all field extraction happens in the bronze layer
- **`filename` as the incremental key** — the filename encodes the extraction hour (e.g. `2026-05-14_2`), parsed as `extract_timestamp`. Source-aligned and stable across reloads, unlike a Snowflake load timestamp
- **`APPEND_ONLY = TRUE` on the Stream** — Amplitude event data is immutable; there are no updates or deletes to capture from the raw table
- **`QUALIFY ROW_NUMBER()`** in `dim_devices` — deduplicates by keeping the most recent device profile, avoiding `SELECT DISTINCT *` which is unreliable on non-unique rows
- **`IS DISTINCT FROM`** in the `dim_devices` MERGE — prevents unnecessary updates when device attributes haven't changed, avoiding silent overwrites of unchanged rows
- **`FORCE = FALSE`** on COPY INTO — prevents re-ingestion of already-loaded files without manual deduplication
- **Separate stored procedures per layer** — each procedure can fail independently without stopping the others, making it easier to diagnose and rerun a specific layer
- **Task chaining with `AFTER`** — silver and gold tasks are triggered by completion of the preceding task rather than on their own schedule, ensuring layers are always processed in order and only when the previous layer has new data
