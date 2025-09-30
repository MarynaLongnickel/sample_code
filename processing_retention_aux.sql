processing_retention_aux.sql
WITH gateway_group AS (
    SELECT
        type,
        CASE
            WHEN type = 'wepay' THEN 'WePay'
            WHEN type = 'stripe' THEN 'Stripe'
            WHEN type = 'adyen' THEN 'Adyen'
            ELSE 'Other'
        END AS gateway_group
    FROM {{ ref('dim_gateway') }}
    GROUP BY 1, 2
),

temp AS (
    SELECT
        t.account_id,
        a.name AS account_name,
        am.first_name || ' ' || am.last_name AS account_manager_name,
        a.organization_id,
        f.product,
        gg.gateway_group,
        DATE_TRUNC('month', t.date_created)::date AS date,
        SUM(
            CASE
                WHEN t.transaction_type = 1 THEN t.amount * COALESCE((tr.rate::decimal(20, 10)), CASE WHEN gg.gateway_group = 'WePay' THEN 0.0053729985439988 ELSE 0.001074863799773 END) * er.rate
                WHEN t.transaction_type = 2 THEN -t.amount * COALESCE((tr.rate::decimal(20, 10)), CASE WHEN gg.gateway_group = 'WePay' THEN 0.0053729985439988 ELSE 0.001074863799773 END) * er.rate
                ELSE 0
            END
        ) AS monthly_total
    FROM {{ ref('dim_transaction') }} AS t
    LEFT JOIN {{ ref('dim_account') }} AS a
        ON t.account_id = a.id
    LEFT JOIN {{ ref('dim_form') }} AS f
        ON t.form_id = f.id
    LEFT JOIN {{ ref('dim_gateway') }} AS g
        ON t.gateway_id = g.id
    LEFT JOIN gateway_group AS gg
        ON g.type = gg.type
    LEFT JOIN {{ ref('dim_account_managers_account') }} AS ama
        ON t.account_id = ama.account_id
    LEFT JOIN {{ ref('dim_account_managers') }} AS am
        ON ama.account_manager_id = am.id
    LEFT JOIN {{ source('wbx_data', 'exchange_rates') }} AS er
        ON
            t.currency = er.currency
            AND DATE_TRUNC('day', t.date_created)::date = er.date
    LEFT JOIN {{ source('wbx_data', 'take_rates') }} AS tr
        ON
            gg.gateway_group = tr.gateway_group
            AND DATE_TRUNC('month', t.date_created)::date = tr.date
            AND f.product = tr.product
    WHERE t.status = 6
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

monthly_cohort AS (
    SELECT
        account_id,
        product,
        MIN(date) AS monthly_cohort
    FROM temp
    WHERE monthly_total > 0
    GROUP BY 1, 2
),

date_range AS (
    SELECT
        account_id,
        account_name,
        account_manager_name,
        product,
        gateway_group,
        organization_id,
        MIN(date) AS min_date,
        MAX(date) AS max_date
    FROM temp
    GROUP BY 1, 2, 3, 4, 5, 6
),

months AS (
    SELECT DATE_TRUNC('month', full_date)::date AS date
    FROM {{ ref('dim_dates') }}
    WHERE month_day_number = 1
),

account_dates AS (
    SELECT
        x.account_id + x.product + x.gateway_group + m.date AS id,
        x.account_id,
        x.account_name,
        x.account_manager_name,
        x.product,
        x.gateway_group,
        x.organization_id,
        m.date
    FROM date_range AS x
    CROSS JOIN months AS m
    WHERE
        m.date >= x.min_date
        AND m.date <= LEAST(DATEADD('month', 24, x.max_date), GETDATE())
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),

base AS (
    SELECT
        a.id,
        a.account_id,
        a.account_name,
        a.account_manager_name,
        a.product,
        a.gateway_group,
        CASE a.organization_id
            WHEN 1 THEN 'webconnex'
            WHEN 2 THEN 'younglife'
            WHEN 3 THEN 'rschool'
            ELSE 'unknown'
        END AS organization_id,
        (EXTRACT(MONTH FROM a.date))::smallint AS month,
        (EXTRACT(YEAR FROM a.date))::smallint AS year,
        a.date,
        COALESCE(t.monthly_total, 0)::numeric(20, 5) AS monthly_total
    FROM account_dates AS a
    LEFT JOIN temp AS t
        ON
            a.account_id = t.account_id
            AND a.date = t.date
            AND a.product = t.product
            AND a.gateway_group = t.gateway_group
),

monthly_trailing_total AS (
    SELECT
        b1.id,
        b1.account_id,
        b1.product,
        b1.gateway_group,
        b1.date,
        COALESCE(SUM(b2.monthly_total::decimal(15, 3)), 0) AS monthly_trailing_total
    FROM base AS b1
    LEFT JOIN base AS b2
        ON
            b1.account_id = b2.account_id
            AND b1.product = b2.product
            AND b1.gateway_group = b2.gateway_group
            AND b2.date = DATEADD('year', -1, b1.date)
    GROUP BY 1, 2, 3, 4, 5
),

---------------------------------------- TTM ----------------------------------------
ttm_total AS (
    SELECT
        id,
        account_id,
        product,
        gateway_group,
        date,
        COALESCE(SUM(monthly_total) OVER (
            PARTITION BY account_id, product, gateway_group
            ORDER BY date
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ), 0) AS ttm_total
    FROM base
    GROUP BY 1, 2, 3, 4, 5, monthly_total -- noqa
),

ttm_cohort AS (
    SELECT
        account_id,
        product,
        MIN(date) AS ttm_cohort
    FROM ttm_total
    WHERE ttm_total > 0
    GROUP BY 1, 2
),

ttm_trailing_total AS (
    SELECT
        id,
        LAG(ttm.ttm_total, 12) OVER (PARTITION BY account_id, product, gateway_group ORDER BY date)::decimal(15, 3) AS ttm_trailing_total
    FROM ttm_total AS ttm
),

ttm_monthly_aggregate AS (
    SELECT
        b.id,
        SUM(b2.monthly_total) AS monthly_aggregate
    FROM base AS b
    LEFT JOIN ttm_cohort AS ttmc
        ON
            b.account_id = ttmc.account_id
            AND b.product = ttmc.product
    LEFT JOIN base AS b2
        ON
            b.account_id = b2.account_id
            AND b.product = b2.product
            AND b.gateway_group = b2.gateway_group
            AND b2.date < DATEADD('month', 12, ttmc.ttm_cohort)
            AND b2.date > DATEADD('month', -12, b.date)
    GROUP BY 1
),

ttm_total_new_existing AS (
    SELECT
        b.id,
        b.account_id,
        b.product,
        b.gateway_group,
        b.date,
        CASE
            WHEN DATEDIFF(MONTH, ttmc.ttm_cohort, b.date) < 12 THEN ttm.ttm_total
            WHEN DATEDIFF(MONTH, ttmc.ttm_cohort, b.date) >= 12 AND DATEDIFF(MONTH, ttmc.ttm_cohort, b.date) < 24
                THEN ttmag.monthly_aggregate
            ELSE 0
        END AS ttm_total_new,
        COALESCE(ttm.ttm_total, 0) - COALESCE(ttm_total_new, 0) AS ttm_total_existing
    FROM base AS b
    LEFT JOIN ttm_total AS ttm
        ON b.id = ttm.id
    LEFT JOIN ttm_cohort AS ttmc
        ON
            b.account_id = ttmc.account_id
            AND b.product = ttmc.product
    LEFT JOIN ttm_monthly_aggregate AS ttmag
        ON b.id = ttmag.id
    GROUP BY 1, 2, 3, 4, 5, ttmc.ttm_cohort, ttm.ttm_total, ttmag.monthly_aggregate -- noqa
),

---------------------------------------- YTD ----------------------------------------
ytd_total AS (
    SELECT
        b1.id,
        b1.account_id,
        b1.product,
        b1.gateway_group,
        b1.date,
        COALESCE(SUM(b2.monthly_total::decimal(15, 3)), 0) AS ytd_total
    FROM base AS b1
    LEFT JOIN base AS b2
        ON
            b1.account_id = b2.account_id
            AND b1.product = b2.product
            AND b1.gateway_group = b2.gateway_group
            AND b1.month >= b2.month
            AND b1.year = b2.year
    GROUP BY 1, 2, 3, 4, 5
),

ytd_cohort AS (
    SELECT
        account_id,
        product,
        MIN(date) AS ytd_cohort
    FROM ytd_total
    WHERE ytd_total > 0
    GROUP BY 1, 2
),

ytd_trailing_total AS (
    SELECT
        id,
        LAG(ytd.ytd_total, 12) OVER (PARTITION BY account_id, product, gateway_group ORDER BY date)::decimal(15, 3) AS ytd_trailing_total
    FROM ytd_total AS ytd
),

ytd_monthly_aggregate AS (
    SELECT
        b.id,
        SUM(b2.monthly_total) AS monthly_aggregate
    FROM base AS b
    LEFT JOIN ytd_cohort AS ytdc
        ON
            b.account_id = ytdc.account_id
            AND b.product = ytdc.product
    LEFT JOIN base AS b2
        ON
            b.account_id = b2.account_id
            AND b.product = b2.product
            AND b.gateway_group = b2.gateway_group
            AND b2.date < DATEADD('month', 12, ytdc.ytd_cohort)
            AND b2.date > DATEADD('month', -12, b.date)
            AND DATE_PART(YEAR, b2.date) = DATE_PART(YEAR, ytdc.ytd_cohort) + 1
    GROUP BY 1
),

ytd_total_new_existing AS (
    SELECT
        b.id,
        b.account_id,
        b.product,
        b.gateway_group,
        b.date,
        CASE
            WHEN DATEDIFF(MONTH, ytdc.ytd_cohort, b.date) < 12 THEN ytd.ytd_total
            WHEN DATEDIFF(MONTH, ytdc.ytd_cohort, b.date) >= 12 AND DATE_PART(YEAR, b.date) = DATE_PART(YEAR, ytdc.ytd_cohort) + 1
                THEN ytdag.monthly_aggregate
            ELSE 0
        END AS ytd_total_new,
        COALESCE(ytd.ytd_total, 0) - COALESCE(ytd_total_new, 0) AS ytd_total_existing
    FROM base AS b
    LEFT JOIN ytd_total AS ytd
        ON
            b.id = ytd.id
    LEFT JOIN ytd_cohort AS ytdc
        ON
            b.account_id = ytdc.account_id
            AND b.product = ytdc.product
    LEFT JOIN ytd_monthly_aggregate AS ytdag
        ON b.id = ytdag.id
    GROUP BY 1, 2, 3, 4, 5, ytdc.ytd_cohort, ytd.ytd_total, ytdag.monthly_aggregate -- noqa
)

---------------------------------- Main ----------------------------------

SELECT
    b.account_id,
    b.account_name,
    b.account_manager_name,
    b.product,
    b.gateway_group,
    mc.monthly_cohort,
    tc.ttm_cohort,
    yc.ytd_cohort,
    b.organization_id,
    b.date,
    b.month,
    b.year,
    COALESCE(b.monthly_total, 0) AS monthly_total,
    COALESCE(mtt.monthly_trailing_total, 0) AS monthly_trailing_total,
    COALESCE(b.monthly_total, 0) - COALESCE(mtt.monthly_trailing_total, 0) AS change_in_existing_monthly,
    COALESCE(ttm.ttm_total, 0) AS ttm_total,
    COALESCE(ttne.ttm_total_new, 0) AS ttm_total_new,
    COALESCE(ttne.ttm_total_existing, 0) AS ttm_total_existing,
    COALESCE(ttt.ttm_trailing_total, 0) AS ttm_trailing_total,
    COALESCE(ttne.ttm_total_existing, 0) - COALESCE(ttt.ttm_trailing_total, 0) AS change_in_existing_ttm,
    COALESCE(ytd.ytd_total, 0) AS ytd_total,
    COALESCE(ytne.ytd_total_new, 0) AS ytd_total_new,
    COALESCE(ytne.ytd_total_existing, 0) AS ytd_total_existing,
    COALESCE(ytt.ytd_trailing_total, 0) AS ytd_trailing_total,
    COALESCE(ytne.ytd_total_existing, 0) - COALESCE(ytt.ytd_trailing_total, 0) AS change_in_existing_ytd
FROM base AS b
LEFT JOIN monthly_trailing_total AS mtt
    ON b.id = mtt.id
LEFT JOIN ttm_total AS ttm
    ON b.id = ttm.id
LEFT JOIN ttm_trailing_total AS ttt
    ON b.id = ttt.id
LEFT JOIN ytd_total AS ytd
    ON b.id = ytd.id
LEFT JOIN ytd_trailing_total AS ytt
    ON b.id = ytt.id
LEFT JOIN ttm_total_new_existing AS ttne
    ON b.id = ttne.id
LEFT JOIN ytd_total_new_existing AS ytne
    ON b.id = ytne.id
LEFT JOIN monthly_cohort AS mc
    ON
        b.account_id = mc.account_id
        AND b.product = mc.product
LEFT JOIN ttm_cohort AS tc
    ON
        b.account_id = tc.account_id
        AND b.product = tc.product
LEFT JOIN ytd_cohort AS yc
    ON
        b.account_id = yc.account_id
        AND b.product = yc.product
