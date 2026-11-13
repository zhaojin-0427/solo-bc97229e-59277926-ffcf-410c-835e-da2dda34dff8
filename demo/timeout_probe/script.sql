-- Expensive recursive CTE: with the 2-second per-probe limit every probe
-- is interrupted and the job must be marked incomplete.
WITH RECURSIVE cnt(x) AS (
    SELECT 1
    UNION ALL
    SELECT x + 1 FROM cnt
    CROSS JOIN (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
                UNION ALL SELECT 4)
    WHERE x < 18000000
)
SELECT count(*) FROM cnt;
