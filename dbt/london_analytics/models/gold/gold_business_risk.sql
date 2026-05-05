-- Model: gold_business_risk
-- Description: Grain is one row per active London company. Assigns a crime risk score and tier to businesses.
-- `risk_tier` is the primary field for Power BI filtering; `is_high_risk` enables simple counts.

WITH
companies AS (
    SELECT
        company_number,
        company_name,
        postcode,
        postcode_district,
        address_line1,
        post_town,
        company_category,
        company_age_years,
        company_age_band,
        sic_code_primary,
        sic_code_secondary,
        incorporation_year
    FROM {{ ref('silver_companies_london') }}
),

crime AS (
    SELECT
        lsoa_code,
        lad_name,
        crimes_per_1000_residents,
        crime_rate_tier,
        total_population,
        imd_decile,
        deprivation_category
    FROM {{ ref('gold_crime_per_capita') }}
    WHERE crime_year = 2023 -- restrict to most recent year
),

joined AS (
    -- approximate join: postcode_district (outward code) matched to lsoa_code (approximate)
    SELECT
        c.company_number,
        c.company_name,
        c.postcode,
        c.postcode_district,
        c.address_line1,
        c.post_town,
        c.company_category,
        c.company_age_years,
        c.company_age_band,
        c.sic_code_primary,
        c.sic_code_secondary,
        c.incorporation_year,
        g.lsoa_code,
        g.lad_name,
        g.crimes_per_1000_residents,
        g.crime_rate_tier,
        g.total_population,
        g.imd_decile,
        g.deprivation_category
    FROM companies c
    LEFT JOIN crime g
        ON c.postcode_district = g.lsoa_code
),

scored AS (
    SELECT
        j.*,
        -- risk_score is determined by thresholds on crimes_per_1000_residents
        CASE
            WHEN j.crimes_per_1000_residents IS NULL THEN NULL
            WHEN j.crimes_per_1000_residents < 20   THEN 1
            WHEN j.crimes_per_1000_residents < 50   THEN 2
            WHEN j.crimes_per_1000_residents < 100  THEN 3
            WHEN j.crimes_per_1000_residents < 200  THEN 4
            ELSE 5
        END AS risk_score,
        -- map numeric score to descriptive tier; NULL -> 'Unscored'
        CASE
            WHEN j.crimes_per_1000_residents IS NULL THEN 'Unscored'
            WHEN j.crimes_per_1000_residents < 20   THEN 'Very Low Risk'
            WHEN j.crimes_per_1000_residents < 50   THEN 'Low Risk'
            WHEN j.crimes_per_1000_residents < 100  THEN 'Medium Risk'
            WHEN j.crimes_per_1000_residents < 200  THEN 'High Risk'
            ELSE 'Very High Risk'
        END AS risk_tier,
        -- risk_confidence: flag low-population areas where rates are less reliable
        CASE
            WHEN j.crimes_per_1000_residents IS NULL THEN 'No Data'
            WHEN j.total_population < 500 THEN 'Low Confidence'
            ELSE 'High Confidence'
        END AS risk_confidence,
        CASE WHEN (
            CASE
                WHEN j.crimes_per_1000_residents IS NULL THEN NULL
                WHEN j.crimes_per_1000_residents < 20   THEN 1
                WHEN j.crimes_per_1000_residents < 50   THEN 2
                WHEN j.crimes_per_1000_residents < 100  THEN 3
                WHEN j.crimes_per_1000_residents < 200  THEN 4
                ELSE 5
            END
        ) >= 4 THEN 1 ELSE 0 END AS is_high_risk
    FROM joined j
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
        company_age_years,
        company_age_band,
        sic_code_primary,
        sic_code_secondary,
        incorporation_year,
        lsoa_code,
        lad_name,
        crimes_per_1000_residents,
        crime_rate_tier,
        risk_score,
        risk_tier,
        risk_confidence,
        is_high_risk,
        imd_decile,
        deprivation_category
    FROM scored
)

SELECT
    company_number,
    company_name,
    postcode,
    postcode_district,
    address_line1,
    post_town,
    company_category,
    company_age_years,
    company_age_band,
    sic_code_primary,
    sic_code_secondary,
    incorporation_year,
    lsoa_code,
    lad_name,
    crimes_per_1000_residents,
    crime_rate_tier,
    risk_score,
    risk_tier,
    risk_confidence,
    is_high_risk,
    imd_decile,
    deprivation_category
FROM final;
