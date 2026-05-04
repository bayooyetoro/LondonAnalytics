-- Model: silver_companies_london
-- Description: Silver-level company records for London; one row per active company in London.
-- Grain: one row per active London company (deduplicated by company_number, keeping most recent incorporation_date).

WITH
source AS (
    SELECT
        company_name,
        company_number,
        reg_address_address_line1,
        reg_address_post_town,
        reg_address_post_code,
        company_category,
        company_status,
        incorporation_date,
        incorporation_year,
        sic_code_sic_text_1,
        sic_code_sic_text_2
    FROM {{ source('bronze', 'companies_house') }}
),

renamed AS (
    SELECT
        LTRIM(RTRIM(company_number))                     AS company_number,
        LTRIM(RTRIM(company_name))                       AS company_name,
        UPPER(LTRIM(RTRIM(reg_address_post_code)))       AS postcode,
        LTRIM(RTRIM(reg_address_address_line1))          AS address_line1,
        LTRIM(RTRIM(reg_address_post_town))              AS post_town,
        company_category                                 AS company_category,
        company_status                                   AS company_status,
        incorporation_date                                AS incorporation_date,
        incorporation_year                                AS incorporation_year,
        LTRIM(RTRIM(sic_code_sic_text_1))                AS sic_code_primary,
        LTRIM(RTRIM(sic_code_sic_text_2))                AS sic_code_secondary
    FROM source
),

filtered AS (
    SELECT
        *
    FROM renamed
    WHERE
        company_status = 'Active'
        AND postcode IS NOT NULL
        AND (
              LEFT(postcode, 1) IN ('E','N','W') -- single-letter areas included
           OR LEFT(postcode, 2) IN ('EC','NW','SE','SW','WC') -- two-letter areas
        )
),

enriched AS (
    SELECT
        *,
        CASE
            WHEN CHARINDEX(' ', postcode) > 0 THEN LEFT(postcode, CHARINDEX(' ', postcode) - 1)
            ELSE postcode
        END AS postcode_district, -- outward code (e.g. 'SW1A')
        DATEDIFF(year, incorporation_date, GETDATE()) AS company_age_years,
        CASE
            WHEN DATEDIFF(year, incorporation_date, GETDATE()) <= 2 THEN '0-2 years'
            WHEN DATEDIFF(year, incorporation_date, GETDATE()) BETWEEN 3 AND 5 THEN '3-5 years'
            WHEN DATEDIFF(year, incorporation_date, GETDATE()) BETWEEN 6 AND 10 THEN '6-10 years'
            WHEN DATEDIFF(year, incorporation_date, GETDATE()) BETWEEN 11 AND 20 THEN '11-20 years'
            WHEN DATEDIFF(year, incorporation_date, GETDATE()) > 20 THEN '20+ years'
            ELSE NULL
        END AS company_age_band
    FROM filtered
),

deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY company_number ORDER BY COALESCE(incorporation_date, '1900-01-01') DESC) AS rn -- keep most recent incorporation_date
    FROM enriched
),

final AS (
    SELECT
        company_number,
        company_name,
        postcode,
        postcode_district,
        address_line1,
        post_town,
        company_category,
        company_status,
        incorporation_date,
        incorporation_year,
        company_age_years,
        company_age_band,
        sic_code_primary,
        sic_code_secondary
    FROM deduped
    WHERE rn = 1
)

SELECT
    company_number,
    company_name,
    postcode,
    postcode_district,
    address_line1,
    post_town,
    company_category,
    company_status,
    incorporation_date,
    incorporation_year,
    company_age_years,
    company_age_band,
    sic_code_primary,
    sic_code_secondary
FROM final;
