-- Model: gold_crime_by_type
-- Description: Grain is one row per LSOA + crime type + year + quarter. Supports crime type breakdown and trend visualisations.

WITH
source AS (
    SELECT
        lsoa_code,
        lsoa_name,
        crime_year,
        crime_quarter,
        crime_type,
        crime_id
    FROM {{ ref('silver_police_street') }}
),

agg AS (
    SELECT
        lsoa_code,
        lsoa_name,
        crime_year,
        crime_quarter,
        crime_type,
        COUNT(crime_id)                                       AS crime_count,
        CAST(COUNT(crime_id) AS FLOAT) / 3.0                  AS monthly_avg -- average per month within the quarter
    FROM source
    GROUP BY
        lsoa_code,
        lsoa_name,
        crime_year,
        crime_quarter,
        crime_type
),

windowed AS (
    SELECT
        a.lsoa_code,
        a.lsoa_name,
        a.crime_year,
        a.crime_quarter,
        a.crime_type,
        a.crime_count,
        a.monthly_avg,
        -- numeric quarter for correct chronological ordering in window functions
        CASE
            WHEN a.crime_quarter = 'Q1' THEN 1
            WHEN a.crime_quarter = 'Q2' THEN 2
            WHEN a.crime_quarter = 'Q3' THEN 3
            WHEN a.crime_quarter = 'Q4' THEN 4
            ELSE 0
        END                                                     AS quarter_num,
        -- cumulative sum of crimes within LSOA, crime_type and year ordered by quarter
        SUM(a.crime_count) OVER (
            PARTITION BY a.lsoa_code, a.crime_type, a.crime_year
            ORDER BY CASE
                       WHEN a.crime_quarter = 'Q1' THEN 1
                       WHEN a.crime_quarter = 'Q2' THEN 2
                       WHEN a.crime_quarter = 'Q3' THEN 3
                       WHEN a.crime_quarter = 'Q4' THEN 4
                       ELSE 0
                     END
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                                        AS cumulative_crimes_ytd,
        -- rank crime types within each LSOA-year by frequency
        RANK() OVER (
            PARTITION BY a.lsoa_code, a.crime_year
            ORDER BY a.crime_count DESC
        )                                                        AS crime_type_rank,
        -- percent of the LSOA's yearly crimes accounted for by this crime_type
        CAST(a.crime_count AS FLOAT) / NULLIF(SUM(a.crime_count) OVER (PARTITION BY a.lsoa_code, a.crime_year), 0) * 100 AS pct_of_lsoa_crimes
    FROM agg a
)

SELECT
    lsoa_code,
    lsoa_name,
    crime_year,
    crime_quarter,
    crime_type,
    crime_count,
    monthly_avg,
    cumulative_crimes_ytd,
    crime_type_rank,
    pct_of_lsoa_crimes
FROM windowed;
