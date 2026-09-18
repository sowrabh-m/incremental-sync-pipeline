# Incremental Sync Pipeline — Dynamic Tables vs. Stream+Task+MERGE

The same incremental sync built two ways on the same source table, to compare complexity, correctness, and cost — not just explain the difference, but have actually run both.

## Architecture

```
SOURCE_ITEMS (raw table)
   │
   ├─▶ Stream (SOURCE_ITEMS_STREAM) ─▶ Task (1-min schedule, MERGE) ─▶ TARGET_STREAM_TASK
   │
   └─▶ Dynamic Table (TARGET_LAG = 1 min, auto-managed)  ─▶ TARGET_DYNAMIC_TABLE
```

## Approach A — Stream + Task + MERGE (manual)

```sql
CREATE STREAM SOURCE_ITEMS_STREAM ON TABLE SOURCE_ITEMS;

CREATE TABLE TARGET_STREAM_TASK (
  ITEM_ID INT, ITEM_NAME STRING, PRICE NUMBER(10,2), UPDATED_AT TIMESTAMP_NTZ
);

CREATE TASK SYNC_TASK
  WAREHOUSE = DE_INCSYNC_WH
  SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('SOURCE_ITEMS_STREAM')
AS
MERGE INTO TARGET_STREAM_TASK t
USING (SELECT * FROM SOURCE_ITEMS_STREAM WHERE METADATA$ACTION = 'INSERT') s
ON t.ITEM_ID = s.ITEM_ID
WHEN MATCHED THEN UPDATE SET
  t.ITEM_NAME = s.ITEM_NAME, t.PRICE = s.PRICE, t.UPDATED_AT = s.UPDATED_AT
WHEN NOT MATCHED THEN INSERT (ITEM_ID, ITEM_NAME, PRICE, UPDATED_AT)
  VALUES (s.ITEM_ID, s.ITEM_NAME, s.PRICE, s.UPDATED_AT);

ALTER TASK SYNC_TASK RESUME;
```

**Why filter to `METADATA$ACTION = 'INSERT'` only:** a standard Stream represents an `UPDATE` as a paired `DELETE` + `INSERT` (old row removed, new row inserted, both flagged `METADATA$ISUPDATE = TRUE`). Acting on both actions in one `MERGE` tries to touch the same target row twice in a single statement, which Snowflake rejects. Filtering to just `INSERT`-action rows correctly captures new inserts *and* the new values from updates, while skipping the redundant "old value removed" row.

## Approach B — Dynamic Table (declarative)

```sql
CREATE DYNAMIC TABLE TARGET_DYNAMIC_TABLE
  TARGET_LAG = '1 MINUTE'
  WAREHOUSE = DE_INCSYNC_WH
AS
SELECT ITEM_ID, ITEM_NAME, PRICE, UPDATED_AT
FROM SOURCE_ITEMS;
```

One statement. No stream, no target table DDL, no hand-written MERGE, no update-vs-insert edge case to reason about — Snowflake manages the whole sync lifecycle.

## Refresh mode — verified, not assumed

```sql
SHOW DYNAMIC TABLES LIKE 'TARGET_DYNAMIC_TABLE';
SELECT REFRESH_START_TIME, STATE, REFRESH_TRIGGER, REFRESH_ACTION
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(NAME=>'TARGET_DYNAMIC_TABLE'))
ORDER BY REFRESH_START_TIME DESC;
```

`refresh_mode = INCREMENTAL` (Snowflake's automatic choice, since the defining query is a plain passthrough `SELECT` with no aggregation — a query that couldn't be computed incrementally, e.g. one with a non-deterministic function or certain aggregations, would force `FULL` instead). Refresh history confirms only 3 of ~40 scheduled 1-minute checks did real work (`INCREMENTAL`, at creation + after the insert + after the update) — every other check returned `NO_DATA` and was skipped cheaply, same efficiency idea as the Task's `WHEN SYSTEM$STREAM_HAS_DATA` guard.

## Comparison

| | Stream + Task + MERGE | Dynamic Table |
|---|---|---|
| Objects to create | Stream, target table, Task | One `CREATE DYNAMIC TABLE` |
| Merge logic | Hand-written, must handle the update-as-delete+insert edge case | Automatic |
| Refresh mode | N/A (task always runs the full MERGE when triggered) | Auto-selected (`INCREMENTAL` here, verified above) |
| Control | Full control over merge logic, conditions, error handling | Less control, but far less code to maintain |
| Best fit | Complex transformation logic during sync (not just a passthrough) | Straightforward transformations where Snowflake's optimizer can go incremental |

## A real mistake, kept in for the record

First attempt at the Task's `MERGE` had `METADATA$ACTION 'INSERT'` — missing the `=`. Snowflake's parser read this as an invalid literal (`SQL compilation error: Unsupported data type literal`). The task failed on 3 consecutive scheduled runs, then Snowflake **auto-suspended it** (`FAILED_AND_AUTO_SUSPENDED`) — a real safety behavior worth knowing: it stops a broken task from silently burning warehouse credits forever. Since every failed run rolled back without consuming the Stream's offset, fixing the `MERGE` and resuming the task picked up both the original insert and the update correctly in one go — no data was lost.

## Setup

Snowflake-only — no AWS/Airflow needed. Warehouse/database/role: `DE_INCSYNC_WH` / `DE_INCSYNC_DB` / `DE_INCSYNC_ROLE`, same account as prior projects. Needs `GRANT EXECUTE TASK ON ACCOUNT` for the Task's role — easy to miss and then wonder why a Task never fires.
