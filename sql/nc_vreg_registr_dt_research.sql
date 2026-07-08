-- This script is to suss out a pattern/reason for why there are registr_dt of "##/##/####"
-- It doesn't appear to be random: ~47% come from one county (26) across
-- all voter statuses, and counts spike on year-start/post-election snapshot
-- dates. Root cause still unconfirmed; likely a county-level or batch
-- processing quirk, not evenly distributed noise.

SELECT data_date, COUNT(*) AS "record_count" FROM public.nc_vreg_history
WHERE registr_dt = '##/##/####'
GROUP BY data_date
ORDER BY record_count DESC;

SELECT data_date, COUNT(*) AS "record_count" FROM public.nc_vreg_history
WHERE registr_dt = '##/##/####'
GROUP BY data_date
ORDER BY data_date DESC;

SELECT * FROM public.nc_vreg_history
WHERE registr_dt = '##/##/####'
ORDER BY data_date DESC;

SELECT
    county_id,
    status_cd,
    voter_status_desc,
    reason_cd,
    voter_status_reason_desc,
    COUNT(*) AS record_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_placeholder_rows
FROM public.nc_vreg_history
WHERE registr_dt = '##/##/####'
GROUP BY county_id, status_cd, voter_status_desc, reason_cd, voter_status_reason_desc
ORDER BY record_count DESC;
