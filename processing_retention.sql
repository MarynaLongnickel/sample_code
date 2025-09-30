WITH
product_totals AS (
    SELECT
        account_id,
        product,
        date,
        SUM(monthly_total) AS monthly_total_product,
        SUM(ttm_total) AS ttm_total_product,
        SUM(ytd_total) AS ytd_total_product,
        SUM(monthly_trailing_total) AS monthly_trailing_total_product,
        SUM(ttm_trailing_total) AS ttm_trailing_total_product,
        SUM(ytd_trailing_total) AS ytd_trailing_total_product
    FROM {{ ref('processing_retention_aux') }}
    GROUP BY 1, 2, 3
),

monthly_future_total AS (
    SELECT
        b.account_id,
        b.product,
        b.date,
        MAX(b2.date) AS future_date
    FROM {{ ref('processing_retention_aux') }} AS b
    LEFT JOIN {{ ref('processing_retention_aux') }} AS b2
        ON
            b.account_id = b2.account_id
            AND b.product = b2.product
            AND b.date < b2.date
    LEFT JOIN product_totals AS pt
        ON
            b2.account_id = pt.account_id
            AND b2.product = pt.product
            AND b2.date = pt.date
    WHERE pt.monthly_total_product > 0
    GROUP BY 1, 2, 3
),

ttm_future_total AS (
    SELECT
        b.account_id,
        b.product,
        b.date,
        MAX(b2.date) AS future_date
    FROM {{ ref('processing_retention_aux') }} AS b
    LEFT JOIN {{ ref('processing_retention_aux') }} AS b2
        ON
            b.account_id = b2.account_id
            AND b.product = b2.product
            AND b.date < b2.date
    LEFT JOIN product_totals AS pt
        ON
            b2.account_id = pt.account_id
            AND b2.product = pt.product
            AND b2.date = pt.date
    WHERE pt.ttm_total_product > 0
    GROUP BY 1, 2, 3
),

ytd_future_total AS (
    SELECT
        b.account_id,
        b.product,
        b.date,
        MAX(b2.date) AS future_date
    FROM {{ ref('processing_retention_aux') }} AS b
    LEFT JOIN {{ ref('processing_retention_aux') }} AS b2
        ON
            b.account_id = b2.account_id
            AND b.product = b2.product
            AND b.date < b2.date
    LEFT JOIN product_totals AS pt
        ON
            b2.account_id = pt.account_id
            AND b2.product = pt.product
            AND b2.date = pt.date
    WHERE pt.ytd_total_product > 0
    GROUP BY 1, 2, 3
),

customer_status AS (
    SELECT
        ra.account_id,
        ra.product,
        ra.gateway_group,
        ra.date,
        CASE
            WHEN pt.monthly_total_product > 0 AND pt.monthly_trailing_total_product <= 0 AND DATEDIFF('month', ra.monthly_cohort, ra.date) < 12 THEN 'new'
            WHEN pt.monthly_total_product <= 0 AND pt.monthly_trailing_total_product > 0 AND m.future_date IS NULL THEN 'lost'
            WHEN pt.monthly_total_product <= 0 AND pt.monthly_trailing_total_product > 0 AND m.future_date IS NOT NULL THEN 'lapsed'
            WHEN pt.monthly_total_product > 0 AND pt.monthly_trailing_total_product <= 0 AND DATEDIFF('month', ra.monthly_cohort, ra.date) >= 12 THEN 'recovered'
            WHEN ra.monthly_total > 0 AND pt.monthly_trailing_total_product > 0 AND ra.change_in_existing_monthly >= 0 THEN 'upsell'
            WHEN pt.monthly_total_product > 0 AND ra.monthly_trailing_total > 0 AND ra.change_in_existing_monthly < 0 THEN 'downsell'
            WHEN pt.monthly_total_product <= 0 AND pt.monthly_trailing_total_product <= 0 THEN 'negative or null'
            ELSE 'unknown'
        END AS monthly_status,
        CASE
            WHEN pt.ttm_total_product > 0 AND pt.ttm_trailing_total_product <= 0 AND DATEDIFF('month', ra.ttm_cohort, ra.date) < 12 THEN 'new'
            WHEN pt.ttm_total_product <= 0 AND pt.ttm_trailing_total_product > 0 AND t.future_date IS NULL THEN 'lost'
            WHEN pt.ttm_total_product <= 0 AND pt.ttm_trailing_total_product > 0 AND t.future_date IS NOT NULL THEN 'lapsed'
            WHEN pt.ttm_total_product > 0 AND pt.ttm_trailing_total_product <= 0 AND DATEDIFF('month', ra.ttm_cohort, ra.date) >= 12 THEN 'recovered'
            WHEN ra.ttm_total > 0 AND (pt.ttm_trailing_total_product > 0 OR ra.ttm_total_new > 0) AND ra.change_in_existing_ttm >= 0 THEN 'upsell'
            WHEN pt.ttm_total_product > 0 AND (ra.ttm_trailing_total > 0 OR ra.ttm_total_new > 0) AND (ra.change_in_existing_ttm < 0 OR ra.ttm_total <= 0) THEN 'downsell'
            WHEN pt.ttm_total_product <= 0 AND pt.ttm_trailing_total_product <= 0 THEN 'negative or null'
            ELSE 'unknown'
        END AS ttm_status,
        CASE
            WHEN pt.ytd_total_product > 0 AND pt.ytd_trailing_total_product <= 0 AND DATEDIFF('month', ra.ytd_cohort, ra.date) < 12 THEN 'new'
            WHEN pt.ytd_total_product <= 0 AND pt.ytd_trailing_total_product > 0 AND y.future_date IS NULL THEN 'lost'
            WHEN pt.ytd_total_product <= 0 AND pt.ytd_trailing_total_product > 0 AND y.future_date IS NOT NULL THEN 'lapsed'
            WHEN pt.ytd_total_product > 0 AND pt.ytd_trailing_total_product <= 0 AND DATEDIFF('month', ra.ytd_cohort, ra.date) >= 12 THEN 'recovered'
            WHEN ra.ytd_total > 0 AND (pt.ytd_trailing_total_product > 0 OR ra.ytd_total_new > 0) AND ra.change_in_existing_ytd >= 0 THEN 'upsell'
            WHEN pt.ytd_total_product > 0 AND (ra.ytd_trailing_total > 0 OR ra.ytd_total_new > 0) AND (ra.change_in_existing_ytd < 0 OR ra.ytd_total <= 0) THEN 'downsell'
            WHEN pt.ytd_total_product <= 0 AND pt.ytd_trailing_total_product <= 0 THEN 'negative or null'
            ELSE 'unknown'
        END AS ytd_status
    FROM {{ ref('processing_retention_aux') }} AS ra
    LEFT JOIN product_totals AS pt
        ON
            ra.account_id = pt.account_id
            AND ra.product = pt.product
            AND ra.date = pt.date
    LEFT JOIN monthly_future_total AS m
        ON
            ra.account_id = m.account_id
            AND ra.product = m.product
            AND ra.date = m.date
    LEFT JOIN ttm_future_total AS t
        ON
            ra.account_id = t.account_id
            AND ra.product = t.product
            AND ra.date = t.date
    LEFT JOIN ytd_future_total AS y
        ON
            ra.account_id = y.account_id
            AND ra.product = y.product
            AND ra.date = y.date
),

processing_max_ttm AS (
    SELECT
        account_id,
        product,
        MAX(ttm_total) AS max_total,
        CASE
            WHEN max_total > 2500000 THEN '1. Strategic (>$2.5m)'
            WHEN max_total > 1000000 THEN '2. Very Large ($1m - $2.5m)'
            WHEN max_total > 250000 THEN '3. Large ($250k - $1m)'
            WHEN max_total > 100000 THEN '4. Medium ($100k - $250k)'
            WHEN max_total > 25000 THEN '5. Small ($25k - $100k)'
            WHEN max_total > 10000 THEN '6. Very Small ($10k - $25k)'
            WHEN max_total > 0 THEN '7. Micro ($1 - $10k)'
            ELSE '8. Non Active (<= $0)'
        END AS processing_max_ttm_size
    FROM {{ ref('processing_retention_aux') }}
    GROUP BY 1, 2
),

customer_size_py AS (
    SELECT
        account_id,
        product,
        year,
        customer_size_py
    FROM {{ ref('processing_volume_retention') }}
    GROUP BY 1, 2, 3, 4
)

SELECT
    c.account_id,
    c.account_name,
    c.account_manager_name,
    c.product,
    c.monthly_cohort,
    c.ttm_cohort,
    c.ytd_cohort,
    c.organization_id,
    c.gateway_group,
    i.industry,
    c.date,
    c.month,
    c.year,
    c.monthly_total,
    c.monthly_trailing_total,
    c.change_in_existing_monthly,
    pt.monthly_total_product,
    pt.monthly_trailing_total_product,
    c.ttm_total,
    c.ttm_total_new,
    c.ttm_total_existing,
    c.ttm_trailing_total,
    c.change_in_existing_ttm,
    pt.ttm_total_product,
    pt.ttm_trailing_total_product,
    c.ytd_total,
    c.ytd_total_new,
    c.ytd_total_existing,
    c.ytd_trailing_total,
    c.change_in_existing_ytd,
    pt.ytd_total_product,
    pt.ytd_trailing_total_product,
    s.monthly_status,
    s.ttm_status,
    s.ytd_status,
    COALESCE(a.utm_medium, 'organic') AS attribution_type,
    COALESCE(CASE WHEN
        LOWER(a.utm_source) = 'facebook'
        OR LOWER(a.utm_source) = 'fb'
        OR LOWER(a.utm_source) = 'instagram' THEN 'meta'
    ELSE a.utm_source END, 'unknown') AS attribution_source,
    m.product_max_ttm_size,
    COALESCE(pmax.processing_max_ttm_size, '8. Non Active (<= $0)') AS processing_max_ttm_size,
    COALESCE(cspy.customer_size_py, '8. Non Active (<= $0)') AS customer_size_py
FROM {{ ref('processing_retention_aux') }} AS c
LEFT JOIN customer_status AS s
    ON
        c.account_id = s.account_id
        AND c.product = s.product
        AND c.gateway_group = s.gateway_group
        AND c.date = s.date
LEFT JOIN {{ ref('account_attribution_pivot') }} AS a
    ON c.account_id = a.account_id
LEFT JOIN customer_size_py AS cspy
    ON
        c.account_id = cspy.account_id
        AND c.product = cspy.product
        AND c.year = cspy.year
LEFT JOIN {{ source('wbx_data', 'industries_master') }} AS i
    ON c.account_id = i.account_id
LEFT JOIN product_totals AS pt
    ON
        c.account_id = pt.account_id
        AND c.product = pt.product
        AND c.date = pt.date
LEFT JOIN {{ ref('max_ttm') }} AS m
    ON
        c.account_id = m.account_id
        AND c.product = m.product
LEFT JOIN processing_max_ttm AS pmax
    ON
        c.account_id = pmax.account_id
        AND c.product = pmax.product
