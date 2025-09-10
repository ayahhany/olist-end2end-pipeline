{{ config(
    materialized='table',
    unique_key='mql_id'
) }}

WITH leads_qualified AS (
    SELECT * FROM {{ ref('stg_leads_qualified_ayahany') }}
),

leads_closed AS (
    SELECT * FROM {{ ref('stg_leads_closed_ayahany') }}
),

final AS (
    SELECT
        lq.mql_id,
        lq.first_contact_date,
        lq.landing_page_id,
        lq.origin,
        lc.seller_id,
        lc.won_date,
        lc.business_segment,
        lc.declared_monthly_revenue,
        lq.updated_at_timestamp
    FROM leads_qualified lq
    LEFT JOIN leads_closed lc
        ON lq.mql_id = lc.mql_id
)

SELECT * FROM final
