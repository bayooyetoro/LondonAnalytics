-- Model: silver_police_street
-- Description: Silver-level street crime records cleaned, typed and deduplicated.
-- Grain: one row per unique crime (deduplicated by crime_id).
-- Key filters: keeps only records with non-null `lsoa_code`, `crime_id`, and LSOAs starting with 'E01' (London subset).

WITH
source AS (
    SELECT
        crime_id,
        month,
        reported_by,
        falls_within,
        longitude,
        latitude,
        location,
        lsoa_code,
        lsoa_name,
        crime_type,
        last_outcome_category
    FROM {{ source('bronze', 'police_street') }}
),

renamed AS (
    SELECT
        crime_id                        AS crime_id,
        month                           AS month_text,
        reported_by                     AS reported_by,
        falls_within                    AS falls_within,
        longitude                       AS longitude_raw,
        latitude                        AS latitude_raw,
        location                        AS location,
        lsoa_code                       AS lsoa_code,
        lsoa_name                       AS lsoa_name,
        crime_type                      AS crime_type,
        last_outcome_category           AS last_outcome
    FROM source
),

cleaned AS (
    SELECT
        LTRIM(RTRIM(crime_id))                                            AS crime_id,
        TRY_CAST(LTRIM(RTRIM(month_text)) + '-01' AS DATE)                 AS crime_date, -- append '-01' to convert YYYY-MM to a DATE
        DATEPART(year, TRY_CAST(LTRIM(RTRIM(month_text)) + '-01' AS DATE)) AS crime_year,
        DATEPART(month, TRY_CAST(LTRIM(RTRIM(month_text)) + '-01' AS DATE))AS crime_month_num,
        LTRIM(RTRIM(crime_type))                                           AS crime_type,
        LTRIM(RTRIM(lsoa_code))                                            AS lsoa_code,
        LTRIM(RTRIM(lsoa_name))                                            AS lsoa_name,
        TRY_CAST(LTRIM(RTRIM(latitude_raw))  AS FLOAT)                     AS latitude,  -- cast coords to FLOAT
        TRY_CAST(LTRIM(RTRIM(longitude_raw)) AS FLOAT)                     AS longitude,
        LTRIM(RTRIM(location))                                              AS location,
        LTRIM(RTRIM(last_outcome))                                          AS last_outcome,
        LTRIM(RTRIM(reported_by))                                           AS reported_by,
        LTRIM(RTRIM(falls_within))                                          AS falls_within
    FROM renamed
),

filtered AS (
    SELECT
        *,
        CASE
            WHEN DATEPART(month, crime_date) IN (1,2,3)    THEN 'Q1'
            WHEN DATEPART(month, crime_date) IN (4,5,6)    THEN 'Q2'
            WHEN DATEPART(month, crime_date) IN (7,8,9)    THEN 'Q3'
            WHEN DATEPART(month, crime_date) IN (10,11,12) THEN 'Q4'
            ELSE NULL
        END AS crime_quarter
    FROM cleaned
    WHERE
        lsoa_code IS NOT NULL
        AND lsoa_code LIKE 'E01%' -- London LSOAs only
        AND crime_id IS NOT NULL
        AND crime_year IN (2021, 2022, 2023) -- restrict to expected years
),

deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY crime_id ORDER BY crime_date ASC) AS rn -- keep first occurrence by earliest date
    FROM filtered
),

final AS (
    SELECT
        crime_id,
        crime_date,
        crime_year,
        crime_month_num,
        crime_quarter,
        crime_type,
        lsoa_code,
        lsoa_name,
        latitude,
        longitude,
        location,
        last_outcome,
        reported_by,
        falls_within
    FROM deduped
    WHERE rn = 1
)

SELECT
    crime_id,
    crime_date,
    crime_year,
    crime_month_num,
    crime_quarter,
    crime_type,
    lsoa_code,
    lsoa_name,
    latitude,
    longitude,
    location,
    last_outcome,
    reported_by,
    falls_within
FROM final;
