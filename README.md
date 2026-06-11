# Amplitude Snowflake

A set of Snowflake SQL scripts that load raw Amplitude event data from AWS S3 and transform it through a medallion architecture (raw → bronze → silver → gold).

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
    │
    ▼
amplitude_events_raw        ← raw: full JSON + filename as VARIANT, loaded via COPY INTO / Snowpipe
    │
    ▼
amplitude_events_base       ← bronze: JSON extracted to typed columns, event types cleaned
    │
    ▼
fct_all_session_events      ← silver: core event fact table
dim_event_pages             ← silver: page and element attributes per event
dim_devices                 ← silver: latest device profile per device_id
    │
    ▼
all_events                  ← gold: fully joined, enriched event-level table
session_journey             ← gold: session-level aggregation with page path and duration
```

---

## File structure

```
amplitude-snowflake/
├── s3_to_snowflake_load.sql      # Raw layer: storage integration, external stage, raw table, COPY INTO, Snowpipe
├── amplitude_bronze.sql          # Bronze layer: amplitude_events_base
├── amplitude_silver.sql          # Silver layer: fct_all_session_events, dim_event_pages, dim_devices
├── amplitude_gold.sql            # Gold layer: all_events, session_journey
└── snowflake_orchestration.sql   # Ongoing load: Stream + INSERT INTO pattern
```

---

## Prerequisites

- An AWS S3 bucket containing Amplitude JSON files (see [Amplitude-API-Project](https://github.com/TIL-harrietowen/Amplitude-API-Project))
- An AWS IAM Role with read access to the S3 bucket
- A Snowflake account with `ACCOUNTADMIN` or `SYSADMIN` privileges to create storage integrations

---

## Layer by layer

### Raw — `s3_to_snowflake_load.sql`

Connects Snowflake to S3 and loads raw JSON files into a staging table.

**Objects created:**

- `<schema>` — dedicated schema for all Amplitude objects
- `<storage-integration>` — authorisation handshake between Snowflake and AWS (uses IAM Role, not IAM User)
- `<file-format>` — JSON file format with `STRIP_OUTER_ARRAY = FALSE` (Amplitude files are NDJSON)
- `<stage>` — external stage pointing to the S3 bucket path
- `amplitude_events_raw` — raw table with two columns: `json_data VARIANT` and `filename VARCHAR`

**Setup steps:**

1. Create the storage integration, then run `DESC INTEGRATION` to retrieve `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`
2. Add these values to the AWS IAM Role's trust policy to complete the two-way handshake
3. Run `LIST @<stage>` to verify the connection
4. Run `COPY INTO` once manually to load all existing files
5. Create the Snowpipe for ongoing automated loads triggered by S3 event notifications

> ⚠️ **Snowpipe vs Snowflake Task:** Snowpipe triggers on every file arrival and is suited for near-real-time loads. Since Amplitude data is only needed once per day, a Snowflake Task on a fixed daily schedule would be more cost-efficient. Snowpipe is implemented here as a course exercise.

---

### Bronze — `amplitude_bronze.sql`

Extracts typed fields from the raw JSON and applies light cleaning. No business logic at this layer.

**Object created:** `amplitude_events_base`

**What it does:**

- Extracts fields from `json_data` using Snowflake's colon notation (e.g. `json_data:event_type::varchar`)
- Extracts nested `event_properties` fields using dot notation with quoted Amplitude field names
- Parses `amplitude_extracted_at` from the filename using `REGEXP_SUBSTR` — this is the source-aligned timestamp used as the incremental key throughout the pipeline
- Cleans `event_type` values: strips `[Amplitude]` prefixes, replaces underscores with spaces, and applies `INITCAP`
- Adds a `load_timestamp` using `current_timestamp()` to record when the row entered Snowflake

**Fields extracted:**

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
| `extract_timestamp` | Parsed from `filename`                                  |
| `load_timestamp`    | `current_timestamp()`                                   |

---

### Silver — `amplitude_silver.sql`

Splits the bronze table into a fact table and two dimension tables following a star schema pattern.

**Objects created:**

**`fct_all_session_events`** — one row per event, core event attributes only

| Field               | Description                                |
| ------------------- | ------------------------------------------ |
| `event_uuid`        | Primary key                                |
| `session_id`        | Session the event belongs to               |
| `device_id`         | Foreign key to `dim_devices`               |
| `event_type`        | Cleaned event type                         |
| `event_time`        | Timestamp of the event                     |
| `extract_timestamp` | When the data was extracted from Amplitude |
| `load_timestamp`    | When the row was loaded into Snowflake     |

**`dim_event_pages`** — page and element context per event (filtered to rows where `page_url` is not null)

| Field               | Description                             |
| ------------------- | --------------------------------------- |
| `event_uuid`        | Foreign key to `fct_all_session_events` |
| `page_url`          | URL of the page                         |
| `page_counter`      | Page view counter within the session    |
| `element_text`      | Text of the clicked/interacted element  |
| `element_tag`       | HTML tag of the element                 |
| `element_url`       | Href of the element                     |
| `extract_timestamp` | Source-aligned timestamp                |
| `load_timestamp`    | Load timestamp                          |

**`dim_devices`** — one row per `device_id`, using the most recent record via `QUALIFY ROW_NUMBER()`

| Field            | Description              |
| ---------------- | ------------------------ |
| `device_id`      | Primary key              |
| `device_type`    | e.g. Desktop, Mobile     |
| `device_family`  | e.g. Mac, iPhone         |
| `platform`       | e.g. Web                 |
| `os_version`     | Operating system version |
| `os_name`        | Operating system name    |
| `load_timestamp` | Load timestamp           |

---

### Gold — `amplitude_gold.sql`

Joins and enriches the silver tables into analysis-ready output tables.

**Objects created:**

**`all_events`** — fully joined, enriched event-level table

Joins `fct_all_session_events`, `dim_event_pages`, and `dim_devices`. Adds:

- `event_duration_s` — time in seconds between this event and the next (`LEAD` window function)
- `event_counter` — sequential event number within the session (`ROW_NUMBER`)
- `previous_event_type` — the preceding event type (`LAG`)
- `previous_page_url` — the preceding page URL (`LAG`)
- `previous_event_repeated` — boolean flag: same event type on the same page as the previous event
- `click_error` — boolean flag: an Element Clicked event preceded by another Element Clicked on the same page (potential rage click / error pattern)

**`session_journey`** — one row per session, session-level aggregation

| Field                | Description                                     |
| -------------------- | ----------------------------------------------- |
| `session_id`         | Primary key                                     |
| `device_id`          | Device used in the session                      |
| `device_type`        | Device type                                     |
| `platform`           | Platform                                        |
| `page_path`          | Ordered list of page titles visited (`LISTAGG`) |
| `total_events`       | Total number of events in the session           |
| `total_pages_viewed` | Number of Page Viewed events                    |
| `session_start_time` | First event timestamp                           |
| `session_end_time`   | Last event timestamp                            |
| `event_duration_s`   | Total session duration in seconds               |

---

### Orchestration — `snowflake_orchestration.sql`

Handles ongoing incremental loads after the initial `COPY INTO`.

**Pattern used:** Stream + `INSERT INTO`

- A Stream (`events_raw_to_base_stream`) is created on `amplitude_events_raw` with `APPEND_ONLY = TRUE`
- The Stream captures only newly loaded rows — it does not reprocess existing data
- An `INSERT INTO amplitude_events_base` statement reads from the Stream, applying the same JSON extraction and cleaning logic as the initial bronze build

> ⚠️ The `INSERT INTO` statement in `snowflake_orchestration.sql` intentionally replaces the `FROM amplitude_events_raw` clause in the bronze script with `FROM events_raw_to_base_stream`. The Stream is consumed on each run, so only new rows are processed.

---

## Running the scripts

Run the scripts in this order:

```
1. s3_to_snowflake_load.sql     — one-time setup + initial load
2. amplitude_bronze.sql         — build amplitude_events_base
3. amplitude_silver.sql         — build fact and dimension tables
4. amplitude_gold.sql           — build analysis-ready output tables
5. snowflake_orchestration.sql  — set up Stream + Snowpipe for ongoing loads
```

For ongoing daily loads, Snowpipe handles the raw → raw table step automatically. The Stream + `INSERT INTO` in `snowflake_orchestration.sql` then picks up new rows into the bronze table. The silver and gold tables would need to be rebuilt or converted to incremental patterns to reflect new data.

---

## Key design decisions

- **`VARIANT` for raw JSON** — schema never needs to change if Amplitude adds new event properties; all extraction happens in the bronze layer
- **`filename` as the incremental key** — the filename encodes the extraction hour (e.g. `2026-05-14_2`), parsed as `extract_timestamp`. This is source-aligned and stable across reloads, unlike a Snowflake load timestamp
- **`QUALIFY ROW_NUMBER()`** in `dim_devices` — deduplicates devices by keeping the most recent record per `device_id`, avoiding `SELECT DISTINCT *` which is unreliable on non-unique rows
- **`FORCE = FALSE`** on COPY INTO — prevents re-ingestion of already-loaded files without needing manual deduplication
- **`APPEND_ONLY = TRUE`** on the Stream — Amplitude event data is immutable; there are no updates or deletes to capture
