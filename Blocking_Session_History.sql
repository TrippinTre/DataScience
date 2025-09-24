-- Blocking session history (last :days days)
-- Shows: timestamp, blocked session/user + SQL_ID, blocker session/user + SQL_ID, wait info
-- Note: Requires access to V$ACTIVE_SESSION_HISTORY and DBA_HIST_ACTIVE_SESS_HISTORY (Diagnostics Pack).

VAR days NUMBER;
BEGIN :days := 4; END;
/

WITH ash_all AS (
  -- Recent samples (in-memory)
  SELECT
    a.sample_time,
    a.instance_number         AS inst_id,
    a.session_id              AS sid,
    a.session_serial#         AS serial#,
    a.user_id,
    a.sql_id,
    a.event,
    a.wait_class,
    a.session_state,
    a.blocking_session        AS blk_sid,
    a.blocking_session_serial# AS blk_serial#,
    a.blocking_session_status AS blk_status
  FROM v$active_session_history a
  WHERE a.sample_time >= SYSDATE - :days

  UNION ALL

  -- Historical samples (AWR / ASH)
  SELECT
    h.sample_time,
    h.instance_number         AS inst_id,
    h.session_id              AS sid,
    h.session_serial#         AS serial#,
    h.user_id,
    h.sql_id,
    h.event,
    h.wait_class,
    h.session_state,
    h.blocking_session        AS blk_sid,
    h.blocking_session_serial# AS blk_serial#,
    h.blocking_session_status AS blk_status
  FROM dba_hist_active_sess_history h
  WHERE h.sample_time >= SYSDATE - :days
),
joined AS (
  SELECT
    a.sample_time,

    -- Blocked side
    a.inst_id           AS blocked_inst_id,
    a.sid               AS blocked_sid,
    a.serial#           AS blocked_serial#,
    u1.username         AS blocked_user,
    a.sql_id            AS blocked_sql_id,
    a.event,
    a.wait_class,
    a.session_state,

    -- Blocker identifiers as recorded on the blocked row
    a.blk_status        AS blocking_status,
    a.blk_sid           AS blocker_sid_reported,
    a.blk_serial#       AS blocker_serial_reported,

    -- Try to find the blocker’s own ASH row near the same time (to fetch blocker SQL_ID, user, inst)
    b.inst_id           AS blocker_inst_id,
    b.sid               AS blocker_sid,
    b.serial#           AS blocker_serial#,
    u2.username         AS blocker_user,
    b.sql_id            AS blocker_sql_id

  FROM ash_all a
  LEFT JOIN dba_users u1 ON u1.user_id = a.user_id

  /* Self-join: match the blocker’s session using SID+SERIAL# within a 1-minute window.
     Widen to 2–3 minutes if your system timestamps are sparse. */
  LEFT JOIN ash_all b
    ON b.sid      = a.blk_sid
   AND b.serial#  = a.blk_serial#
   AND b.sample_time BETWEEN a.sample_time - INTERVAL '1' MINUTE
                         AND a.sample_time + INTERVAL '1' MINUTE
  LEFT JOIN dba_users u2 ON u2.user_id = b.user_id

  WHERE a.blk_sid IS NOT NULL
    AND a.blk_status = 'VALID'
)
SELECT
  sample_time,
  -- Blocked
  blocked_inst_id,
  blocked_sid,
  blocked_serial#,
  blocked_user,
  blocked_sql_id,

  -- Blocker (from both the blocked row and matched blocker row)
  NVL(blocker_inst_id, blocked_inst_id) AS blocker_inst_id, -- fallback if inst unknown
  COALESCE(blocker_sid, blocker_sid_reported)       AS blocker_sid,
  COALESCE(blocker_serial#, blocker_serial_reported) AS blocker_serial#,
  blocker_user,
  blocker_sql_id,

  -- Wait info
  wait_class,
  event,
  blocking_status
FROM joined
ORDER BY sample_time, blocked_inst_id, blocked_sid;
