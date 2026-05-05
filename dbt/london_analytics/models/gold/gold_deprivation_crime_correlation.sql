-- Model: gold_deprivation_crime_correlation
-- Description: Grain is one row per London LSOA for 2023. Primary insight model combining IMD, crime and domain ranks.
-- `composite_vulnerability_score` combines IMD, crime, health, employment and education into a 0-1 vulnerability index.
-- `imd_crime_agreement` flags LSOAs where deprivation and crime agree or diverge.

WITH
crime AS (
    SELECT
        lsoa_code,
        lsoa_name,
        lad_name,
        crime_year,
        total_crimes,
        total_population,
        crimes_per_1000_residents,
        crime_rate_tier,
        imd_decile,
        deprivation_category
    FROM {{ ref('gold_crime_per_capita') }}
),

deprivation AS (
    SELECT
        lsoa_code,
        imd_rank,
        imd_decile,
        income_decile,
        employment_decile,
        health_decile,
        education_decile,
        crime_deprivation_decile,
        living_env_decile,
        deprivation_category
    FROM {{ ref('silver_deprivation') }}
),

joined AS (
    SELECT
        c.*,
        d.imd_rank                                                              AS imd_rank_src,
        d.imd_decile                                                            AS imd_decile_src,
        d.income_decile                                                         AS income_decile_src,
        d.health_decile                                                         AS health_decile_src,
        d.employment_decile                                                     AS employment_decile_src,
        d.education_decile                                                      AS education_decile_src,
        d.crime_deprivation_decile                                              AS crime_deprivation_decile_src,
        d.living_env_decile                                                     AS living_env_decile_src,
        d.deprivation_category                                                  AS deprivation_category_src
    FROM crime c
    LEFT JOIN deprivation d
        ON c.lsoa_code = d.lsoa_code
    WHERE c.crime_year = 2023 -- filter to most recent full year
),

ranked AS (
    SELECT
        j.*,
        RANK() OVER (ORDER BY crimes_per_1000_residents DESC)                   AS crimes_per_1000_residents_rank -- rank areas by crime rate (1 = highest crime rate)
    FROM joined j
),

normed AS (
    SELECT
        r.*,
        -- normalise ranks to 0-1 by dividing by the max rank across the dataset
        CASE WHEN MAX(imd_rank_src) OVER () IS NULL THEN NULL ELSE CAST(imd_rank_src AS FLOAT) / NULLIF(MAX(imd_rank_src) OVER (), 0) END AS imd_rank_norm,
        CASE WHEN MAX(crimes_per_1000_residents_rank) OVER () IS NULL THEN NULL ELSE CAST(crimes_per_1000_residents_rank AS FLOAT) / NULLIF(MAX(crimes_per_1000_residents_rank) OVER (), 0) END AS crime_rank_norm,
        CASE WHEN MAX(health_decile_src) OVER () IS NULL THEN NULL ELSE CAST(health_decile_src AS FLOAT) / NULLIF(MAX(health_decile_src) OVER (), 0) END AS health_rank_norm,
        CASE WHEN MAX(employment_decile_src) OVER () IS NULL THEN NULL ELSE CAST(employment_decile_src AS FLOAT) / NULLIF(MAX(employment_decile_src) OVER (), 0) END AS employment_rank_norm,
        CASE WHEN MAX(education_decile_src) OVER () IS NULL THEN NULL ELSE CAST(education_decile_src AS FLOAT) / NULLIF(MAX(education_decile_src) OVER (), 0) END AS education_rank_norm
    FROM ranked r
),

scored AS (
    SELECT
        n.lsoa_code,
        n.lsoa_name,
        n.lad_name,
        n.total_crimes,
        n.total_population,
        n.crimes_per_1000_residents,
        n.crime_rate_tier,
        n.imd_rank_src                                                          AS imd_rank,
        n.imd_decile_src                                                        AS imd_decile,
        n.deprivation_category_src                                              AS deprivation_category,
        n.income_decile_src                                                     AS income_decile,
        n.health_decile_src                                                     AS health_decile,
        n.employment_decile_src                                                 AS employment_decile,
        n.education_decile_src                                                  AS education_decile,
        n.crime_deprivation_decile_src                                          AS crime_deprivation_decile,
        n.living_env_decile_src                                                 AS living_env_decile,
        -- deprivation_crime_divergence: absolute difference between IMD rank and crime rank
        ABS(n.imd_rank_src - n.crimes_per_1000_residents_rank)                  AS deprivation_crime_divergence,
        -- composite_vulnerability_score: weighted average of normalised ranks (0-1 where 1 = most vulnerable)
        (0.3 * COALESCE(n.imd_rank_norm, 0.0))
        + (0.25 * COALESCE(n.crime_rank_norm, 0.0))
        + (0.2 * COALESCE(n.health_rank_norm, 0.0))
        + (0.15 * COALESCE(n.employment_rank_norm, 0.0))
        + (0.1 * COALESCE(n.education_rank_norm, 0.0))                         AS composite_vulnerability_score,
        n.crimes_per_1000_residents_rank                                        AS crimes_per_1000_residents_rank
    FROM normed n
),

final AS (
    SELECT
        s.lsoa_code,
        s.lsoa_name,
        s.lad_name,
        s.total_crimes,
        s.total_population,
        s.crimes_per_1000_residents,
        s.crime_rate_tier,
        s.imd_rank,
        s.imd_decile,
        s.deprivation_category,
        s.income_decile,
        s.health_decile,
        s.employment_decile,
        s.education_decile,
        s.crime_deprivation_decile,
        s.living_env_decile,
        s.composite_vulnerability_score,
        CASE
            WHEN s.composite_vulnerability_score >= 0.8 THEN 'Critical'
            WHEN s.composite_vulnerability_score >= 0.6 THEN 'High'
            WHEN s.composite_vulnerability_score >= 0.4 THEN 'Medium'
            WHEN s.composite_vulnerability_score >= 0.2 THEN 'Low'
            ELSE 'Minimal'
        END AS vulnerability_tier,
        s.deprivation_crime_divergence,
        -- imd_crime_agreement categorises agreement/divergence between IMD decile and crime tier
        CASE
            WHEN s.imd_decile <= 3 AND s.crime_rate_tier IN ('High','Very High') THEN 'Deprived and High Crime'
            WHEN s.imd_decile <= 3 AND s.crime_rate_tier IN ('Low','Medium')     THEN 'Deprived but Lower Crime'
            WHEN s.imd_decile >= 8 AND s.crime_rate_tier IN ('High','Very High') THEN 'Affluent but High Crime'
            WHEN s.imd_decile >= 8 AND s.crime_rate_tier IN ('Low','Medium')     THEN 'Affluent and Lower Crime'
            ELSE 'Mixed'
        END AS imd_crime_agreement
    FROM scored s
),

deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY lsoa_code ORDER BY composite_vulnerability_score DESC) AS rn -- keep the most vulnerable record if duplicates exist
    FROM final
)

SELECT
    lsoa_code,
    lsoa_name,
    lad_name,
    total_crimes,
    total_population,
    crimes_per_1000_residents,
    crime_rate_tier,
    imd_rank,
    imd_decile,
    deprivation_category,
    income_decile,
    health_decile,
    employment_decile,
    education_decile,
    crime_deprivation_decile,
    living_env_decile,
    composite_vulnerability_score,
    vulnerability_tier,
    deprivation_crime_divergence,
    imd_crime_agreement
FROM deduped
WHERE rn = 1;
