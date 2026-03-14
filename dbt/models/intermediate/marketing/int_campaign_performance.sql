WITH campaigns AS (
    SELECT * FROM {{ ref('stg_campaigns') }}
),

-- Mocking daily performance metrics (clicks, impressions, spend)
-- In a real scenario, this would reference a staging model for performance data
performance_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['campaign_id', 'date_day']) }} AS performance_id,
        campaign_id,
        date_day,
        impressions,
        clicks,
        spend
    FROM {{ ref('stg_campaign_performance_raw') }} -- Placeholder for raw performance
),

joined AS (
    SELECT
        c.campaign_id,
        c.campaign_name,
        c.campaign_status,
        p.date_day,
        p.impressions,
        p.clicks,
        p.spend,
        CASE
            WHEN p.impressions > 0 THEN (p.clicks / p.impressions) * 100
            ELSE 0
        END AS click_through_rate,
        CASE
            WHEN p.clicks > 0 THEN p.spend / p.clicks
            ELSE 0
        END AS cost_per_click
    FROM campaigns c
    LEFT JOIN performance_data p
        ON c.campaign_id = p.campaign_id
)

SELECT * FROM joined
