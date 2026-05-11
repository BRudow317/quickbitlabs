# DWH.MQ_PKG - Master Data Management Message Queue Package

Handles inbound MDM record ingestion, enrichment, hash-based delta detection, and async
multi-threaded processing. Records enter via mq_inbound, are enriched and compared against
the source view, then applied to the DWH demographic, address, and phone tables.

---

## Schema Objects

| Object | Type | Description |
|---|---|---|
| dwh.mq_inbound | Table | Staging table for inbound MQ records |
| dwh.mq_outbound | Table | Outbound snapshot for external delivery |
| dwh.mq_pkg_log | Table | Autonomous transaction error and audit log |
| dwh.mq_source_vw | View | Live hash of current DWH state for delta comparison |
| global_mq_id_sequence | Sequence | Shared PK sequence across mq_inbound and mq_outbound |

---

## Status Codes

All status values reference qbl.http_codes and follow HTTP conventions.

| Code | Meaning |
|---|---|
| 202 | Accepted - new record, pending processing |
| 200 | OK - processed, at least one domain changed and was applied |
| 201 | Queued - reserved for priority batch targeting |
| 204 | No Content - employee not in this system, ignored |
| 304 | Not Modified - processed, no changes detected |
| 404 | Not Found - employee_id missing from payload |
| 500 | Internal Server Error - row-level failure, see mq_pkg_log |

---

## Setup

### build_multi_threader

Creates or updates the DBMS_SCHEDULER jobs that back async processing. Call once at
deploy time or any time you need to change the interval, chunk size, or thread count.

```sql
PROCEDURE build_multi_threader (
    i_job_name        VARCHAR2 := 'MQ_INBOUND_THREADER',
    i_job_type        VARCHAR2 := 'PLSQL_BLOCK',
    i_schema          VARCHAR2 := 'DWH',
    i_pkg_name        VARCHAR2 := 'MQ_PKG',
    i_prc_name        VARCHAR2 := 'PROCESS_MQ_INBOUND',
    i_thread_count    NUMBER   := 4,
    i_chunk_size      NUMBER   := 0,
    i_status_type     NUMBER   := 202,
    i_repeat_interval VARCHAR2 := NULL,
    i_enabled         BOOLEAN  := TRUE,
    i_auto_drop       BOOLEAN  := FALSE
)
```

**i_chunk_size** - 0 means process everything available each run. A non-zero value caps
total records processed per run, divided evenly across threads.

**i_repeat_interval** - Oracle scheduler calendar syntax. NULL means the jobs are one-shot
and must be triggered manually via assign_thread or bulk_process_mq_inbound.

**Example - standard recurring setup:**
```sql
BEGIN
    dwh.mq_pkg.build_multi_threader(
        i_repeat_interval => 'FREQ=MINUTELY;INTERVAL=2'
    );
END;
```

**Example - 4 threads, process up to 100k records every 5 minutes:**
```sql
BEGIN
    dwh.mq_pkg.build_multi_threader(
        i_thread_count    => 4,
        i_chunk_size      => 100000,
        i_repeat_interval => 'FREQ=MINUTELY;INTERVAL=5'
    );
END;
```

**Example - update interval on existing jobs:**
```sql
BEGIN
    dwh.mq_pkg.build_multi_threader(
        i_repeat_interval => 'FREQ=MINUTELY;INTERVAL=1'
    );
END;
```

---

## Loading

### mq_json_loader

Bulk-inserts a JSON array payload into mq_inbound and immediately signals workers.
Falls back to mq_json_rbr_loader automatically on any bulk failure.

```sql
PROCEDURE mq_json_loader (i_payload IN CLOB)
```

**Example:**
```sql
BEGIN
    dwh.mq_pkg.mq_json_loader(
        i_payload => '[
            {"employee_id": "abc123", "first_name": "Jane", "last_name": "Doe",
             "gender": "FEMALE", "mar_status": "SINGLE",
             "phone": "5551234567", "address1": "123 Main St",
             "city": "Springfield", "state": "IL", "postal": "62701",
             "country": "USA"}
        ]'
    );
END;
```

---

### mq_json_rbr_loader

Row-by-row fallback loader. Called automatically by mq_json_loader on failure. Can be
called directly when you need per-row error isolation from the start.

```sql
PROCEDURE mq_json_rbr_loader (i_payload IN CLOB)
```

**Example:**
```sql
BEGIN
    dwh.mq_pkg.mq_json_rbr_loader(i_payload => :json_clob);
END;
```

---

### mq_inbound_loader

Single-record insert for use when you already have a populated mq_inbound%ROWTYPE.
Called internally by mq_json_rbr_loader. Useful for programmatic record construction.

```sql
PROCEDURE mq_inbound_loader (mqi_record IN dwh.mq_inbound%ROWTYPE)
```

**Example:**
```sql
DECLARE
    v_rec dwh.mq_inbound%ROWTYPE;
BEGIN
    v_rec.employee_id := 'abc123';
    v_rec.first_name  := 'Jane';
    v_rec.last_name   := 'Doe';
    v_rec.mq_status   := 202;
    dwh.mq_pkg.mq_inbound_loader(v_rec);
END;
```

---

## Processing

### bulk_process_mq_inbound

Entry point for kicking off async processing. Calls build_multi_threader to stamp the
current chunk/status configuration into the job actions, then fires all threads.

Use this when you want to trigger an immediate run outside the scheduled interval, or
when targeting a specific status code (such as 201 for a priority batch).

```sql
PROCEDURE bulk_process_mq_inbound (
    i_chunk_size   IN NUMBER   := 0,
    i_thread_count IN NUMBER   := 4,
    i_status_type  IN NUMBER   := 202,
    i_job_name     IN VARCHAR2 := 'MQ_INBOUND_THREADER'
)
```

**Example - process all pending records now:**
```sql
BEGIN
    dwh.mq_pkg.bulk_process_mq_inbound;
END;
```

**Example - priority batch targeting pre-tagged records:**
```sql
-- Tag the specific records you want
UPDATE dwh.mq_inbound SET mq_status = 201
WHERE employee_id IN ('abc123', 'def456', 'ghi789');
COMMIT;

-- Run workers against status 201 only
BEGIN
    dwh.mq_pkg.bulk_process_mq_inbound(i_status_type => 201);
END;
```

**Example - process exactly 50,000 records across 4 threads:**
```sql
BEGIN
    dwh.mq_pkg.bulk_process_mq_inbound(i_chunk_size => 50000);
END;
```

---

### process_mq_inbound

The worker procedure executed by each scheduler thread. Reads mq_inbound records
partitioned by ora_hash, enriches lookup fields via the cursor, computes MD5 hashes,
and applies changes to demographic, address, and phone tables using MERGE.

Direct calls are useful for single-threaded debugging or synchronous processing.

```sql
PROCEDURE process_mq_inbound (
    i_thread_id      IN NUMBER,
    i_starting_mq_id IN NUMBER  := 0,
    i_chunk_size     IN NUMBER  := 0,
    i_thread_count   IN NUMBER  := 4,
    i_status_type    IN NUMBER  := 202,
    i_bulk_mode      IN BOOLEAN := FALSE
)
```

**i_thread_id** - 1-based thread identifier. Records are partitioned by
ora_hash(mq_id, thread_count - 1) = thread_id - 1.

**i_bulk_mode** - when TRUE, calls census_sentinel after each changed record to keep the
census snapshot current and prevent nightly delta detection bypass.

**Example - run single-threaded for debugging:**
```sql
BEGIN
    dwh.mq_pkg.process_mq_inbound(i_thread_id => 1, i_thread_count => 1);
END;
```

**Example - run thread 2 of 4 against a priority status:**
```sql
BEGIN
    dwh.mq_pkg.process_mq_inbound(
        i_thread_id   => 2,
        i_thread_count => 4,
        i_status_type  => 201
    );
END;
```

---

### assign_thread

Fires all scheduler jobs immediately without waiting for the next scheduled interval.
Silently skips any job that is already running (-27478) or disabled (-27431).

```sql
PROCEDURE assign_thread (
    i_job_name     VARCHAR2 := 'MQ_INBOUND_THREADER',
    i_thread_count NUMBER   := 4
)
```

**Example:**
```sql
BEGIN
    dwh.mq_pkg.assign_thread;
END;
```

---

## Outbound

### mq_outbound_loader

Loads the full current DWH state into mq_outbound from mq_source_vw. Optionally
truncates the table first for a full reload.

```sql
PROCEDURE mq_outbound_loader (
    ilb_reload BOOLEAN  := FALSE,
    iov_schema VARCHAR2 := 'DWH',
    iov_table  VARCHAR2 := 'MQ_OUTBOUND'
)
```

**Example - incremental load:**
```sql
BEGIN
    dwh.mq_pkg.mq_outbound_loader;
END;
```

**Example - full reload:**
```sql
BEGIN
    dwh.mq_pkg.mq_outbound_loader(ilb_reload => TRUE);
END;
```

---

### mq_outbound_employment_loader

Stub pending outbound employment spec. Signature matches mq_outbound_loader pattern.

```sql
PROCEDURE mq_outbound_employment_loader (
    ilb_reload BOOLEAN  := FALSE,
    iov_schema VARCHAR2 := 'DWH',
    iov_table  VARCHAR2 := 'MQ_OUTBOUND_EMPLOYMENT'
)
```

---

## Utilities

### mq_truncate

Truncates a table by schema and name after verifying it exists in all_tables.
Used internally by outbound loaders but safe to call directly.

```sql
PROCEDURE mq_truncate (
    ilv_schema VARCHAR2 := 'DWH',
    ilv_table  VARCHAR2 := NULL
)
```

**Example:**
```sql
BEGIN
    dwh.mq_pkg.mq_truncate(ilv_table => 'MQ_INBOUND');
END;
```

---

### mqi_to_json

Serializes a dwh.mq_inbound%ROWTYPE record to a pretty-printed JSON CLOB.
Used internally by mq_logger and mq_json_rbr_loader for error capture.

```sql
FUNCTION mqi_to_json (mqi_record IN dwh.mq_inbound%ROWTYPE) RETURN CLOB
```

**Example:**
```sql
DECLARE
    v_rec  dwh.mq_inbound%ROWTYPE;
    v_json CLOB;
BEGIN
    SELECT * INTO v_rec FROM dwh.mq_inbound WHERE mq_id = 12345;
    v_json := dwh.mq_pkg.mqi_to_json(v_rec);
    DBMS_OUTPUT.PUT_LINE(v_json);
END;
```

---

### mq_logger

Autonomous transaction error logger. Commits independently of the calling transaction
so log entries survive a rollback. Called automatically by all procedures on failure.

```sql
PROCEDURE mq_logger (
    i_mq_id                 IN NUMBER   DEFAULT NULL,
    i_json_record           IN CLOB     DEFAULT NULL,
    i_error_code            IN NUMBER   DEFAULT NULL,
    i_error_message         IN VARCHAR2 DEFAULT NULL,
    i_error_location        IN VARCHAR2 DEFAULT NULL,
    i_mq_status_code        IN NUMBER   DEFAULT NULL,
    i_procedure_name        IN VARCHAR2 DEFAULT NULL,
    i_procedure_description IN VARCHAR2 DEFAULT NULL,
    i_package_name          IN VARCHAR2 DEFAULT 'MQ_PKG'
)
```

**Example - querying the log:**
```sql
SELECT log_id, mq_id, error_code, error_message, procedure_name, created_at
FROM dwh.mq_pkg_log
WHERE created_at >= SYSTIMESTAMP - INTERVAL '1' HOUR
ORDER BY created_at DESC;
```

**Example - manual log entry:**
```sql
BEGIN
    dwh.mq_pkg.mq_logger(
        i_mq_id          => 12345,
        i_error_code     => -20001,
        i_error_message  => 'Custom diagnostic note',
        i_procedure_name => 'MY_PROC'
    );
END;
```

---

## Typical Workflows

### First-time deploy

```sql
-- 1. Create scheduler jobs with a 2-minute recurring interval
BEGIN
    dwh.mq_pkg.build_multi_threader(
        i_repeat_interval => 'FREQ=MINUTELY;INTERVAL=2'
    );
END;

-- 2. Verify jobs were created
SELECT job_name, state, repeat_interval, last_run_duration
FROM all_scheduler_jobs
WHERE owner = USER AND job_name LIKE 'MQ_INBOUND_THREADER%';
```

---

### Standard inbound load (JSON payload from MuleSoft or API)

```sql
BEGIN
    dwh.mq_pkg.mq_json_loader(i_payload => :your_json_clob);
    -- Workers are signaled immediately after insert
END;
```

---

### JDBC direct insert (external system, no loader)

Records inserted directly to mq_inbound with mq_status = 202 will be picked up
automatically at the next scheduled interval. To process immediately without waiting:

```sql
BEGIN
    dwh.mq_pkg.assign_thread;
END;
```

---

### Priority batch - process a specific group now

```sql
-- Tag the target records with status 201
UPDATE dwh.mq_inbound
SET mq_status = 201
WHERE employee_id IN (
    SELECT employee_id FROM dwh.employee WHERE account_code = 'ACCT001'
);
COMMIT;

-- Fire workers against status 201
BEGIN
    dwh.mq_pkg.bulk_process_mq_inbound(i_status_type => 201);
END;
```

---

### Check processing results

```sql
SELECT mq_status, COUNT(*) AS record_count
FROM dwh.mq_inbound
GROUP BY mq_status
ORDER BY mq_status;

-- Errors in the last hour
SELECT mq_id, error_code, error_message, procedure_name, created_at
FROM dwh.mq_pkg_log
WHERE created_at >= SYSTIMESTAMP - INTERVAL '1' HOUR
ORDER BY created_at DESC;
```

---

### Reprocess failed records

```sql
-- Reset 500s back to 202 for reprocessing
UPDATE dwh.mq_inbound
SET mq_status = 202, updated_at = SYSTIMESTAMP
WHERE mq_status = 500;
COMMIT;

BEGIN
    dwh.mq_pkg.assign_thread;
END;
```
