# Contoso Finance — Database Operations Playbook

## SQL Server Troubleshooting Procedures

### 1. Wait Statistics Analysis

When investigating performance issues, always start with wait statistics to understand what SQL Server is waiting on.

```sql
SELECT TOP 20
    wait_type,
    waiting_tasks_count,
    wait_time_ms,
    max_wait_time_ms,
    signal_wait_time_ms,
    (wait_time_ms - signal_wait_time_ms) AS resource_wait_time_ms,
    CAST(100.0 * wait_time_ms / SUM(wait_time_ms) OVER() AS DECIMAL(5,2)) AS pct
FROM sys.dm_os_wait_stats
WHERE wait_type NOT IN (
    'SLEEP_TASK', 'BROKER_IO_FLUSH', 'SQLTRACE_BUFFER_FLUSH',
    'CLR_AUTO_EVENT', 'CLR_MANUAL_EVENT', 'LAZYWRITER_SLEEP',
    'CHECKPOINT_QUEUE', 'WAITFOR', 'BROKER_EVENTHANDLER',
    'FT_IFTS_SCHEDULER_IDLE_WAIT', 'XE_DISPATCHER_WAIT',
    'REQUEST_FOR_DEADLOCK_SEARCH', 'LOGMGR_QUEUE',
    'ONDEMAND_TASK_QUEUE', 'BROKER_TRANSMITTER',
    'HADR_FILESTREAM_IOMGR_IOCOMPLETION', 'DIRTY_PAGE_POLL'
)
AND waiting_tasks_count > 0
ORDER BY wait_time_ms DESC
```

**Interpretation:**
- High `LCK_M_*` waits → blocking/deadlock issues
- High `PAGEIOLATCH_*` → disk I/O problems
- High `CXPACKET` → parallelism issues
- High `WRITELOG` → transaction log bottleneck

### 2. Deadlock Detection with Extended Events

If wait statistics show lock-related waits, set up Extended Events to capture deadlocks.

**Important:** Use the SQL Server log directory for .xel file output — it is always writable by the service account. Get it with:
```sql
SELECT CAST(SERVERPROPERTY('ErrorLogFileName') AS NVARCHAR(500));
-- Returns e.g. C:\Program Files\Microsoft SQL Server\MSSQL17.MSSQLSERVER\MSSQL\Log\ERRORLOG
-- Strip the filename to get the directory path for the .xel target.
```

**Step 1: Drop any existing session, then create and start**
```sql
-- Drop if exists (needed for clean re-creation)
IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = 'deadlock_monitor')
BEGIN
    ALTER EVENT SESSION [deadlock_monitor] ON SERVER STATE = STOP;
    DROP EVENT SESSION [deadlock_monitor] ON SERVER;
END;

-- Build path dynamically using the SQL Server log directory
DECLARE @logdir NVARCHAR(500) = CAST(SERVERPROPERTY('ErrorLogFileName') AS NVARCHAR(500));
SET @logdir = LEFT(@logdir, LEN(@logdir) - CHARINDEX('\', REVERSE(@logdir)) + 1);
DECLARE @sql NVARCHAR(MAX) = N'
CREATE EVENT SESSION [deadlock_monitor] ON SERVER
ADD EVENT sqlserver.xml_deadlock_report
ADD TARGET package0.event_file(SET filename=N''' + @logdir + 'deadlocks.xel'', max_file_size=50)
WITH (MAX_MEMORY=4096 KB, EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS, STARTUP_STATE=ON)';
EXEC sp_executesql @sql;

ALTER EVENT SESSION [deadlock_monitor] ON SERVER STATE = START;
```

**Step 2: Read captured deadlocks**
```sql
DECLARE @logdir NVARCHAR(500) = CAST(SERVERPROPERTY('ErrorLogFileName') AS NVARCHAR(500));
SET @logdir = LEFT(@logdir, LEN(@logdir) - CHARINDEX('\', REVERSE(@logdir)) + 1);
DECLARE @glob NVARCHAR(500) = @logdir + 'deadlocks*.xel';
DECLARE @sql NVARCHAR(MAX) = N'
SELECT
    event_data.value(''(event/@timestamp)[1]'', ''datetime2'') AS deadlock_time,
    event_data.value(''(event/data[@name="xml_report"]/value)[1]'', ''nvarchar(max)'') AS deadlock_graph
FROM (
    SELECT CAST(event_data AS XML) AS event_data
    FROM sys.fn_xe_file_target_read_file(''' + @glob + ''', NULL, NULL, NULL)
) AS tab
ORDER BY deadlock_time DESC';
EXEC sp_executesql @sql;
```

### 3. Blocking Chain Analysis

When deadlocks or blocking are detected, trace the full blocking chain.

```sql
WITH BlockingChain AS (
    SELECT
        r.session_id,
        r.blocking_session_id,
        r.wait_type,
        r.wait_time,
        r.wait_resource,
        t.text AS query_text,
        0 AS level
    FROM sys.dm_exec_requests r
    CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
    WHERE r.blocking_session_id != 0

    UNION ALL

    SELECT
        r.session_id,
        r.blocking_session_id,
        r.wait_type,
        r.wait_time,
        r.wait_resource,
        t.text,
        bc.level + 1
    FROM sys.dm_exec_requests r
    CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
    JOIN BlockingChain bc ON r.session_id = bc.blocking_session_id
    WHERE bc.level < 10
)
SELECT * FROM BlockingChain ORDER BY level, session_id;
```

### 4. Index Health Check

After resolving immediate issues, check index fragmentation.

```sql
SELECT
    DB_NAME() AS database_name,
    OBJECT_SCHEMA_NAME(ips.object_id) AS schema_name,
    OBJECT_NAME(ips.object_id) AS table_name,
    i.name AS index_name,
    ips.index_type_desc,
    ips.avg_fragmentation_in_percent,
    ips.page_count,
    ips.record_count
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
WHERE ips.avg_fragmentation_in_percent > 10
AND ips.page_count > 100
ORDER BY ips.avg_fragmentation_in_percent DESC;
```

### 5. Server Resource Utilization

Check overall server health metrics.

```sql
SELECT
    record_id,
    EventTime,
    SQLProcessUtilization AS sql_cpu_pct,
    SystemIdle AS idle_cpu_pct,
    100 - SystemIdle - SQLProcessUtilization AS other_cpu_pct
FROM (
    SELECT
        record.value('(./Record/@id)[1]', 'int') AS record_id,
        record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS SQLProcessUtilization,
        record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS SystemIdle,
        DATEADD(ms, -1 * (ts_now - timestamp), GETDATE()) AS EventTime
    FROM (
        SELECT timestamp, CONVERT(xml, record) AS record, cpu_ticks / (cpu_ticks/ms_ticks) AS ts_now
        FROM sys.dm_os_ring_buffers
        CROSS JOIN sys.dm_os_sys_info
        WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
    ) AS t
) AS cpu_usage
ORDER BY record_id DESC;
```
