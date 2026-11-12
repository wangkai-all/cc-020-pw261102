-- 日门店汇总：基于当天清洗结果汇总门店销售额与订单数
CREATE OR REPLACE TABLE ${target} AS
SELECT
    store_id,
    COUNT(*)        AS order_cnt,
    SUM(amount)     AS sales_amount,
    ${dt} AS dt
FROM ${table:orders_clean}
WHERE dt = ${dt}
GROUP BY store_id;
