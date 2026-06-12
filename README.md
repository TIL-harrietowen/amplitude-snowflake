# Amplitude Snowflake

A set of Snowflake SQL scripts that load raw Amplitude event data from AWS S3 and transform it through a medallion architecture (raw → bronze → silver → gold), with stored procedures orchestrating each layer transition.

This repo is part of a wider pipeline:

| Stage                       | Repo                                                                              |
| --------------------------- | --------------------------------------------------------------------------------- |
| Extract + S3 load           | [Amplitude-API-Project](https://github.com/TIL-harrietowen/Amplitude-API-Project) |
| Snowflake load + transform  | **This repo**                                                                     |
| dbt transform (alternative) | Amplitude-dbt                                                                     |

---

## Architecture

```
S3 (JSON files)
    │   Snowpipe (auto-ingest on file arrival)
    ▼
amplitude_events_raw              ← raw:    full JSON + filename stored as VARIANT
    │   Stream (append-only) → sp_events_raw_to_base()
    ▼
amplitude_events_base             ← bronze: typed fields extracted, event types cleaned
    │   sp_amplitude_silver()
    ▼
fct_all_session_events            ← silver: core event fact table
dim_event_pages                   ← silver: page and element attributes per event
dim_devices                       ← silver: one row per device_id (SCD Type 1 via MERGE)
    │   sp_amplitude_gold()
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
├── snowflake_orchestration.sql     # Ongoing loads: Stream + stored procedures for each layer
└── snowflake_update_strategies.sql # Reference: raw SQL behind each stored procedure (used during development)
```

---

## Prerequisites

- An AWS S3 bucket containing Amplitude JSON files (see [Amplitude-API-Project](https://github.com/TIL-harrietowen/Amplitude-API-Project))
- An AWS IAM Role with read access to the S3 bucket
- A Snowflake account with `ACCOUNTADMIN` or `SYSADMIN` privileges to create storage integrations

---

## Running order

Scripts are run in two phases: an initial one-time build, and then ongoing orchestration for new data.

### Phase 1 — one-time setup and initial build

```
1. s3_to_snowflake_load.sql     — AWS + Snowflake connection setup, raw table creation, initial COPY INTO
2. amplitude_bronze.sql         — builds amplitude_events_base from the raw table
3. amplitude_silver.sql         — builds fct_all_session_events, dim_event_pages, dim_devices
4. amplitude_gold.sql           — builds all_events, session_journey
5. snowflake_orchestration.sql  — creates Stream, Snowpipe, and stored procedures for ongoing loads
```

### Phase 2 — ongoing daily loads

Once the orchestration objects are in place, new data flows automatically:

```
S3 (new JSON files arrive)
    → Snowpipe loads them into amplitude_events_raw
    → Stream captures new rows
    → sp_events_raw_to_base()  — processes stream into amplitude_events_base
    → sp_amplitude_silver()    — inserts/merges new rows into silver tables
    → sp_amplitude_gold()      — inserts new rows into gold tables
```

The stored procedures can be called manually or wired up to a Snowflake Task for scheduled execution.

---

## Layer by layer

### Raw — `s3_to_snowflake_load.sql`

Connects Snowflake to S3 and loads raw JSON into a staging table.

> 💡 **IAM Role vs IAM User** — the Python extraction pipeline uses an IAM User (key + secret). Snowflake uses an IAM Role instead. Access is granted via a trust relationship — no long-lived credentials are stored in Snowflake.

**Setup steps:**

1. Create the storage integration, passing the AWS Role ARN
2. Run `DESC INTEGRATION` to retrieve `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`
3. Add these to the AWS IAM Role's trust policy to complete the two-way handshake
4. Create the file format, external stage, and raw table
5. Run `LIST @<stage>` to verify the connection
6. Run `COPY INTO` once manually to load all existing files
7. Create the Snowpipe for ongoing automated loads

**Objects created:**

| Object                  | Type                | Purpose                                                                |
| ----------------------- | ------------------- | ---------------------------------------------------------------------- |
| `<storage-integration>` | Storage Integration | Authorisation between Snowflake and S3                                 |
| `<file-format>`         | File Format         | JSON parser (`STRIP_OUTER_ARRAY = FALSE` — Amplitude files are NDJSON) |
| `<stage>`               | External Stage      | Live pointer to the S3 path — does not store data                      |
| `amplitude_events_raw`  | Table               | Raw JSON + filename, one row per event                                 |
| `amplitude_events_pipe` | Snowpipe            | Auto-ingests new files from S3 on arrival                              |

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

- Extracts fields from `json_data` using Snowflake colon notation (e.g. `json_data:event_type::varchar`)
- Extracts nested `event_properties` using dot notation with quoted Amplitude field names (e.g. `json_data:event_properties."[Amplitude] Page URL"`)
- Parses `extract_timestamp` from the filename using `REGEXP_SUBSTR` — this is the source-aligned incremental key used throughout all downstream layers
- Cleans `event_type` values: strips `[Amplitude]` prefixes, replaces underscores with spaces, applies `INITCAP`
- Adds `load_timestamp` via `current_timestamp()` to record when the row entered Snowflake

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
| `load_timestamp`    | `current_timestamp()`                                   |

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

**`dim_event_pages`** — page and element context per event (rows where `page_url is not null`)

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

> 💡 `all_events` filters for new rows at the final CTE stage (after window function calculations) because `LEAD` and `LAG` require the full session context to calculate correctly — filtering earlier would produce incorrect values.

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

---

### Orchestration — `snowflake_orchestration.sql`

Sets up the objects and stored procedures that handle ongoing incremental loads after the initial build.

**Objects created:**

| Object                        | Type                          | Purpose                                                                                                      |
| ----------------------------- | ----------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `amplitude_events_raw_stream` | Stream (`APPEND_ONLY = TRUE`) | Captures new rows loaded into `amplitude_events_raw` by Snowpipe                                             |
| `sp_events_raw_to_base()`     | Stored Procedure              | Reads from the stream, applies JSON extraction and cleaning, inserts into `amplitude_events_base`            |
| `sp_amplitude_silver()`       | Stored Procedure              | Inserts new rows into `fct_all_session_events` and `dim_event_pages`; merges into `dim_devices` (SCD Type 1) |
| `sp_amplitude_gold()`         | Stored Procedure              | Inserts new rows into `all_events` and `session_journey`                                                     |

**Incremental load strategies by table:**

| Table                    | Strategy                                    | Why                                                                                                                           |
| ------------------------ | ------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `amplitude_events_base`  | Stream + INSERT                             | Stream ensures only new raw rows are processed — consumed on each run                                                         |
| `fct_all_session_events` | INSERT with `MAX(extract_timestamp)` filter | Append-only — events never change                                                                                             |
| `dim_event_pages`        | INSERT with `MAX(extract_timestamp)` filter | Append-only — page events never change                                                                                        |
| `dim_devices`            | MERGE (SCD Type 1)                          | Device attributes can change — new records inserted, changed records updated, `IS DISTINCT FROM` prevents unnecessary updates |
| `all_events`             | INSERT filtered after window calculations   | `LEAD`/`LAG` require full session context before filtering                                                                    |
| `session_journey`        | INSERT with `MAX(extract_timestamp)` filter | New sessions only — existing sessions are not reopened                                                                        |

**To run the stored procedures manually:**

```sql
CALL sp_events_raw_to_base();
CALL sp_amplitude_silver();
CALL sp_amplitude_gold();
```

---

### Reference — `snowflake_update_strategies.sql`

Contains the raw SQL for each incremental update step, written out as standalone statements. Used during development to test and iterate on the logic before wrapping it into stored procedures. Not intended to be run as part of the regular pipeline.

---

## Key design decisions

- **`VARIANT` for raw JSON** — the schema never needs to change if Amplitude adds new event properties; all field extraction happens in the bronze layer
- **`filename` as the incremental key** — the filename encodes the extraction hour (e.g. `2026-05-14_2`), parsed as `extract_timestamp`. This is source-aligned and stable across reloads, unlike a Snowflake load timestamp
- **`APPEND_ONLY = TRUE` on the Stream** — Amplitude event data is immutable; there are no updates or deletes to capture from the raw table
- **`QUALIFY ROW_NUMBER()`** in `dim_devices` — deduplicates by keeping the most recent device profile, avoiding `SELECT DISTINCT *` which is unreliable on non-unique rows
- **`IS DISTINCT FROM`** in the `dim_devices` MERGE — prevents unnecessary updates when device attribute values haven't actually changed, avoiding silent overwrites of unchanged rows
- **`FORCE = FALSE`** on COPY INTO — prevents re-ingestion of already-loaded files without needing manual deduplication
- **Separate stored procedures per layer** — each procedure can fail independently without stopping the others, making it easier to diagnose and rerun a specific layer
