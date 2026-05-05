-- Model: gold_crime_per_capita
-- Description: Grain is one row per LSOA per year; primary analytical output for crime rate mapping and risk scoring.
-- `crimes_per_1000_residents` is the key metric (crimes per 1,000 residents).

WITH
police AS (
    SELECT
        crime_id,
        crime_date,
        crime_year,
        lsoa_code,
        lsoa_name
    FROM {{ ref('silver_police_street') }}
),

population AS (
    SELECT
        lsoa_code,
        lsoa_name,
        total_population
    FROM {{ ref('silver_population_lsoa') }}
),

deprivation AS (
    SELECT
        lsoa_code,
        lad_name,
        imd_decile,
        deprivation_category
    FROM {{ ref('silver_deprivation') }}
),

aggregated AS (
    SELECT
        p.lsoa_code                                                             AS lsoa_code,
        COALESCE(p.lsoa_name, pop.lsoa_name)                                    AS lsoa_name,
        d.lad_name                                                              AS lad_name,
        p.crime_year                                                            AS crime_year,
        COUNT(p.crime_id)                                                       AS total_crimes,
        MAX(pop.total_population)                                               AS total_population,
        (CAST(COUNT(p.crime_id) AS FLOAT) / NULLIF(MAX(pop.total_population), 0)) * 1000 AS crimes_per_1000_residents,
        CAST(COUNT(p.crime_id) AS FLOAT) / NULLIF(MAX(pop.total_population), 0)  AS crimes_per_resident,
        d.imd_decile                                                             AS imd_decile,
        d.deprivation_category                                                   AS deprivation_category
    FROM police p
    LEFT JOIN population pop
        ON p.lsoa_code = pop.lsoa_code
    LEFT JOIN deprivation d
        ON p.lsoa_code = d.lsoa_code
    GROUP BY
        p.lsoa_code,
        COALESCE(p.lsoa_name, pop.lsoa_name),
        d.lad_name,
        p.crime_year,
        d.imd_decile,
        d.deprivation_category
),

ranked AS (
    SELECT
        *,
        NTILE(4) OVER (ORDER BY crimes_per_1000_residents) AS crime_rate_ntile -- NTILE quartiles across crimes_per_1000_residents
    FROM aggregated
),

final AS (
    SELECT
        lsoa_code,
        lsoa_name,
        lad_name,
        crime_year,
        total_crimes,
        total_population,
        crimes_per_1000_residents,
        crimes_per_resident,
        CASE
            WHEN crime_rate_ntile = 1 THEN 'Low'        -- bottom quartile
            WHEN crime_rate_ntile = 2 THEN 'Medium'     -- 2nd quartile
            WHEN crime_rate_ntile = 3 THEN 'High'       -- 3rd quartile
            WHEN crime_rate_ntile = 4 THEN 'Very High'  -- top quartile
            ELSE NULL
        END AS crime_rate_tier,
        imd_decile,
        deprivation_category
    FROM ranked
)

SELECT
    lsoa_code,
    lsoa_name,
    lad_name,
    crime_year,
    total_crimes,
    total_population,
    crimes_per_1000_residents,
    crimes_per_resident,
    crime_rate_tier,
    imd_decile,
    deprivation_category
FROM final;
