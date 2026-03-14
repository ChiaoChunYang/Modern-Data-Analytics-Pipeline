{{ config(
    materialized='table',
    tags=['marketing', 'daily']
) }}

WITH stg_campaigns AS (
    SELECT * FROM {{ ref('stg_marketing__campaigns') }}
),

stg_ad_spend AS (
    SELECT * FROM {{ ref('stg_marketing__ad_spend') }}
),

stg_conversions AS (
    SELECT * FROM {{ ref('stg_marketing__conversions') }}
),

daily_performance AS (
    SELECT
        stg_campaigns.campaign_id,
        stg_campaigns.campaign_name,
        stg_campaigns.channel,
        stg_ad_spend.date_day,
        SUM(stg_ad_spend.spend) AS daily_spend,
        SUM(stg_ad_spend.impressions) AS daily_impressions,
        SUM(stg_ad_spend.clicks) AS daily_clicks
    FROM stg_campaigns
    JOIN stg_ad_spend ON stg_campaigns.campaign_id = stg_ad_spend.campaign_id
    GROUP BY 1, 2, 3, 4
),

daily_conversions AS (
    SELECT
        campaign_id,
        date_day,
        COUNT(conversion_id) AS total_conversions,
        SUM(revenue) AS total_revenue
    FROM stg_conversions
    GROUP BY 1, 2
),

final AS (
    SELECT
        dp.date_day,
        dp.campaign_id,
        dp.campaign_name,
        dp.channel,
        dp.daily_spend,
        dp.daily_impressions,
        dp.daily_clicks,
        COALESCE(dc.total_conversions, 0) AS total_conversions,
        COALESCE(dc.total_revenue, 0) AS total_revenue,
        
        -- Calculated Metrics
        CASE WHEN dp.daily_clicks > 0 THEN dp.daily_spend / dp.daily_clicks ELSE 0 END AS cpc,
        CASE WHEN dp.daily_spend > 0 THEN dc.total_revenue / dp.daily_spend ELSE 0 END AS roas
    FROM daily_performance dp
    LEFT JOIN daily_conversions dc 
        ON dp.campaign_id = dc.campaign_id 
        AND dp.date_day = dc.date_day
)

SELECT * FROM final
