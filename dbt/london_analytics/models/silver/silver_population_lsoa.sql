-- Model: silver_population_lsoa
-- Description: Aggregated LSOA-level population totals derived from Census 2021 cross-tab counts.
-- Grain: one row per LSOA with `total_population` (sum over all age/sex/qualification combinations).

WITH
source AS (
    SELECT
        lower_layer_super_output_areas_code,
        lower_layer_super_output_areas,
        age_6_categories,
        age_6_categories_code,
        sex_2_categories,
        sex_2_categories_code,
        highest_level_of_qualification_7_categories,
        highest_level_of_qualification_7_categories_code,
        observation
    FROM {{ source('bronze', 'ons_population') }}
),

renamed AS (
    SELECT
        LTRIM(RTRIM(lower_layer_super_output_areas_code)) AS lsoa_code,
        LTRIM(RTRIM(lower_layer_super_output_areas))      AS lsoa_name,
        LTRIM(RTRIM(age_6_categories))                     AS age_6_categories,
        LTRIM(RTRIM(sex_2_categories))                     AS sex_2_categories,
        TRY_CAST(observation AS BIGINT)                    AS population_count
    FROM source
),

filtered AS (
    SELECT
        *
    FROM renamed
    WHERE
        lsoa_code IS NOT NULL
        AND lsoa_code LIKE 'E%' -- England LSOAs only
),

agg_by_sex_age AS (
    SELECT
        lsoa_code,
        lsoa_name,
        sex_2_categories,
        age_6_categories,
        SUM(population_count) AS population_by_sex_age
    FROM filtered
    GROUP BY
        lsoa_code,
        lsoa_name,
        sex_2_categories,
        age_6_categories
),

agg_total AS (
    SELECT
        lsoa_code,
        lsoa_name,
        SUM(population_count) AS total_population
    FROM filtered
    GROUP BY
        lsoa_code,
        lsoa_name
),

final AS (
    SELECT
        lsoa_code,
        lsoa_name,
        total_population,
        CASE
            WHEN total_population < 1000 THEN 'Very Small'
            WHEN total_population BETWEEN 1000 AND 1999 THEN 'Small'
            WHEN total_population BETWEEN 2000 AND 2999 THEN 'Medium'
            WHEN total_population >= 3000 THEN 'Large'
            ELSE NULL
        END AS population_band
    FROM agg_total
)

SELECT
    lsoa_code,
    lsoa_name,
    total_population,
    population_band
FROM final;
