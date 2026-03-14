WITH source AS (
    SELECT * FROM {{ source('raw_marketing', 'campaign_data') }}
),

renamed AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['id', 'campaign_name']) }} AS campaign_id,
        id AS source_id,
        campaign_name,
        campaign_status,
        TRY_CAST(daily_budget AS FLOAT) AS daily_budget,
        TRY_CAST(start_date AS DATE) AS start_date,
        TRY_CAST(end_date AS DATE) AS end_date,
        TO_TIMESTAMP(created_at) AS created_at_utc,
        TO_TIMESTAMP(updated_at) AS updated_at_utc
    FROM source
)

SELECT * FROM renamed
