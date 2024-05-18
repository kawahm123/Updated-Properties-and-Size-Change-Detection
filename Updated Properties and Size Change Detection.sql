WITH ordered_records AS (
    -- Create a ranked list of records ordered by the most recent update dates
    SELECT
        record_id,
        address_line,
        locality,
        region,
        postal_code,
        county,
        category,
        status,
        gross_size,
        row_number() OVER (PARTITION BY record_id ORDER BY valid_to DESC) AS row_num,
        valid_from
    FROM
        records_snapshot_table
),

current_record AS (
    -- Select the most current record details
    SELECT
        record_id AS current_record_id,
        address_line AS current_address_line,
        locality AS current_locality,
        county AS current_county,
        region AS current_region,
        postal_code AS current_postal_code,
        category AS current_category,
        status AS current_status,
        gross_size AS current_gross_size,
        valid_from AS current_valid_from
    FROM
        ordered_records
    WHERE
        row_num = 1 AND
        valid_from >= CURRENT_DATE - INTERVAL '3 months'
),

second_latest_record AS (
    -- Select the second most recent record data
    SELECT
        record_id AS second_record_id,
        gross_size AS second_gross_size
    FROM
        ordered_records
    WHERE
        row_num = 2
),

third_latest_record AS (
    -- Select the third most recent record data
    SELECT
        record_id AS third_record_id
    FROM
        ordered_records
    WHERE
        row_num = 3
)

-- Final query to compare current and second most recent record data
SELECT
    cr.current_record_id,
    cr.current_address_line,
    cr.current_locality,
    cr.current_county,
    cr.current_region,
    cr.current_postal_code,
    cr.current_category,
    cr.current_status,
    slr.second_gross_size,
    cr.current_gross_size,
    ROUND(ABS((cr.current_gross_size - slr.second_gross_size) * 100.0 / NULLIF(slr.second_gross_size, 0)), 1) || '%' AS gross_size_change,
    cr.current_valid_from
FROM
    current_record cr
JOIN
    second_latest_record slr ON cr.current_record_id = slr.second_record_id
LEFT JOIN
    third_latest_record tlr ON cr.current_record_id = tlr.third_record_id
WHERE
    slr.second_record_id IS NOT NULL AND
    ABS((cr.current_gross_size - slr.second_gross_size) * 100.0 / NULLIF(slr.second_gross_size, 0)) >= 25 AND
    cr.current_status IN ('PROPOSED', 'EXISTING', 'UNDER_CONSTRUCTION')