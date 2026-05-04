-- Model: silver_deprivation
-- Description: IMD 2025 deprivation indicators joined to LSOA; deduplicated to one row per LSOA and filtered to London (LAD code prefix E09).
-- Grain: one row per London LSOA.

WITH
source AS (
    SELECT
        postcode,
        lsoa_code_2021,
        lsoa_name_2021,
        lad_code_2024,
        lad_name_2024,
        lsoa_containing_postcode_index_of_multiple_deprivation_imd_rank_where_1_is_most_deprived,
        lsoa_containing_postcode_index_of_multiple_deprivation_imd_decile_where_1_is_most_deprived_10_of_lso_as,
        lsoa_containing_postcode_income_rank_where_1_is_most_deprived,
        lsoa_containing_postcode_income_decile_where_1_is_most_deprived_10_of_lso_as,
        lsoa_containing_postcode_employment_rank_where_1_is_most_deprived,
        lsoa_containing_postcode_employment_decile_where_1_is_most_deprived_10_of_lso_as,
        lsoa_containing_postcode_education_skills_and_training_rank_where_1_is_most_deprived,
        lsoa_containing_postcode_education_skills_and_training_decile_where_1_is_most_deprived_10_of_lso_as,
        lsoa_containing_postcode_health_deprivation_and_disability_rank_where_1_is_most_deprived,
        lsoa_containing_postcode_health_deprivation_and_disability_decile_where_1_is_most_deprived_10_of_lso_as,
        lsoa_containing_postcode_crime_rank_where_1_is_most_deprived,
        lsoa_containing_postcode_crime_decile_where_1_is_most_deprived_10_of_lso_as,
        lsoa_containing_postcode_living_environment_rank_where_1_is_most_deprived,
        lsoa_containing_postcode_living_environment_decile_where_1_is_most_deprived_10_of_lso_as
    FROM {{ source('bronze', 'ons_deprivation') }}
),

renamed AS (
    SELECT
        LTRIM(RTRIM(lsoa_code_2021)) AS lsoa_code,
        LTRIM(RTRIM(lsoa_name_2021)) AS lsoa_name,
        LTRIM(RTRIM(lad_code_2024))  AS lad_code,
        LTRIM(RTRIM(lad_name_2024))  AS lad_name,
        TRY_CAST(lsoa_containing_postcode_index_of_multiple_deprivation_imd_rank_where_1_is_most_deprived AS INT) AS imd_rank,
        TRY_CAST(lsoa_containing_postcode_index_of_multiple_deprivation_imd_decile_where_1_is_most_deprived_10_of_lso_as AS INT) AS imd_decile,
        TRY_CAST(lsoa_containing_postcode_income_rank_where_1_is_most_deprived AS INT) AS income_rank,
        TRY_CAST(lsoa_containing_postcode_income_decile_where_1_is_most_deprived_10_of_lso_as AS INT) AS income_decile,
        TRY_CAST(lsoa_containing_postcode_employment_rank_where_1_is_most_deprived AS INT) AS employment_rank,
        TRY_CAST(lsoa_containing_postcode_employment_decile_where_1_is_most_deprived_10_of_lso_as AS INT) AS employment_decile,
        TRY_CAST(lsoa_containing_postcode_education_skills_and_training_rank_where_1_is_most_deprived AS INT) AS education_rank,
        TRY_CAST(lsoa_containing_postcode_education_skills_and_training_decile_where_1_is_most_deprived_10_of_lso_as AS INT) AS education_decile,
        TRY_CAST(lsoa_containing_postcode_health_deprivation_and_disability_rank_where_1_is_most_deprived AS INT) AS health_rank,
        TRY_CAST(lsoa_containing_postcode_health_deprivation_and_disability_decile_where_1_is_most_deprived_10_of_lso_as AS INT) AS health_decile,
        TRY_CAST(lsoa_containing_postcode_crime_rank_where_1_is_most_deprived AS INT) AS crime_deprivation_rank,
        TRY_CAST(lsoa_containing_postcode_crime_decile_where_1_is_most_deprived_10_of_lso_as AS INT) AS crime_deprivation_decile,
        TRY_CAST(lsoa_containing_postcode_living_environment_rank_where_1_is_most_deprived AS INT) AS living_env_rank,
        TRY_CAST(lsoa_containing_postcode_living_environment_decile_where_1_is_most_deprived_10_of_lso_as AS INT) AS living_env_decile
    FROM source
),

filtered AS (
    SELECT
        *
    FROM renamed
    WHERE
        lad_code LIKE 'E09%' -- London boroughs only
),

deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY lsoa_code ORDER BY COALESCE(imd_rank, 999999) ASC) AS rn -- deduplicate to one row per LSOA; prefer rows with an IMD rank
    FROM filtered
),

final AS (
    SELECT
        lsoa_code,
        lsoa_name,
        lad_code,
        lad_name,
        imd_rank,
        imd_decile,
        income_rank,
        income_decile,
        employment_rank,
        employment_decile,
        education_rank,
        education_decile,
        health_rank,
        health_decile,
        crime_deprivation_rank,
        crime_deprivation_decile,
        living_env_rank,
        living_env_decile,
        CASE
            WHEN imd_decile IN (1,2) THEN 'Highly Deprived'
            WHEN imd_decile IN (3,4) THEN 'Deprived'
            WHEN imd_decile IN (5,6) THEN 'Average'
            WHEN imd_decile IN (7,8) THEN 'Low Deprivation'
            WHEN imd_decile IN (9,10) THEN 'Least Deprived'
            ELSE NULL
        END AS deprivation_category
    FROM deduped
    WHERE rn = 1
)

SELECT
    lsoa_code,
    lsoa_name,
    lad_code,
    lad_name,
    imd_rank,
    imd_decile,
    income_rank,
    income_decile,
    employment_rank,
    employment_decile,
    education_rank,
    education_decile,
    health_rank,
    health_decile,
    crime_deprivation_rank,
    crime_deprivation_decile,
    living_env_rank,
    living_env_decile,
    deprivation_category
FROM final;
