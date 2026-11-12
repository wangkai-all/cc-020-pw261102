-- 近三日滚动指标：显式读取当天及前两天的门店日汇总
CREATE OR REPLACE TABLE ${target} AS
WITH base AS (
    SELECT store_id, order_cnt, sales_amount, dt
    FROM ${table:store_daily}
    WHERE dt IN (${dt}, ${dt_minus_1}, ${dt_minus_2})
)
SELECT
    store_id,
    SUM(order_cnt)     AS rolling_order_cnt_3d,
    SUM(sales_amount)  AS rolling_sales_3d,
    COUNT(DISTINCT dt) AS covered_days,
    ${dt} AS dt
FROM base
GROUP BY store_id
HAVING COUNT(DISTINCT dt) = 3;
