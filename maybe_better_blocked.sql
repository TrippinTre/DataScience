-- Minimal blocking history: just IDs, users, and SQL text (last :days days)
-- Requires: V$ACTIVE_SESSION_HISTORY, DBA_HIST_ACTIVE_SESS_HISTORY, V$SQLAREA, DBA_HIST_SQLTEXT
VAR days NUMBER;
BEGIN :days := 4; END;
/

WITH ash_all AS (
  SELECT sample_time,
         instance_number inst_id,
         session_id      sid,
         session_serial# serial#,
         user_id,
         sql_id,
         blocking_session        blk_sid,
         blocking_session_serial# blk_serial#,
         blocking_session_status  blk_status
  FROM   v$active_session_history
  WHERE  sample_time >= SYSDATE - :days
  UNION ALL
  SELECT sample_time,
         instance_number,
         session_id,
         session_serial#,
         user_id,
         sql_id,
         blocking_session,
         blocking_session_serial#,
         blocking_session_status
  FROM   dba_hist_active_sess_history
  WHERE  sample_time >= SYSDATE - :days
),
-- Prefer current SQL text; fallback to historical (first 4000 chars)
sql_texts AS (
  SELECT sql_id, sql_text, 1 pref FROM v$sqlarea
  UNION ALL
  SELECT sql_id, DBMS_LOB.SUBSTR(sql_text, 4000, 1), 2 FROM dba_hist_sqltext
),
sql_texts_pref AS (
  SELECT sql_id, sql_text
  FROM (
    SELECT sql_id, sql_text,
           ROW_NUMBER() OVER (PARTITION BY sql_id ORDER BY pref) rn
    FROM   sql_texts
  )
  WHERE rn = 1
),
j AS (
  /* For each blocked sample, find the blocker’s own ASH row near the same time to get its SQL_ID/user */
  SELECT a.sample_time,
         a.sid              AS blocked_sid,
         a.serial#          AS blocked_serial#,
         u1.username        AS blocked_user,
         a.sql_id           AS blocked_sql_id,
         st1.sql_text       AS blocked_sql_text,
         COALESCE(b.sid, a.blk_sid)        AS blocker_sid,
         COALESCE(b.serial#, a.blk_serial#) AS blocker_serial#,
         u2.username        AS blocker_user,
         b.sql_id           AS blocker_sql_id,
         st2.sql_text       AS blocker_sql_text
  FROM   ash_all a
  LEFT JOIN dba_users u1 ON u1.user_id = a.user_id
  LEFT JOIN ash_all b
         ON b.sid = a.blk_sid
        AND b.serial# = a.blk_serial#
        AND b.sample_time BETWEEN a.sample_time - INTERVAL '1' MINUTE
                              AND a.sample_time + INTERVAL '1' MINUTE
  LEFT JOIN dba_users u2 ON u2.user_id = b.user_id
  LEFT JOIN sql_texts_pref st1 ON st1.sql_id = a.sql_id
  LEFT JOIN sql_texts_pref st2 ON st2.sql_id = b.sql_id
  WHERE  a.blk_sid IS NOT NULL
    AND  a.blk_status = 'VALID'
)
SELECT
  blocker_sid,
  blocker_serial#,
  blocker_user,
  blocker_sql_id,
  blocker_sql_text,
  blocked_sid,
  blocked_serial#,
  blocked_user,
  blocked_sql_id,
  blocked_sql_text
FROM j
ORDER BY sample_time, blocked_sid;
