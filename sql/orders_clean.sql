-- 日订单清洗：按天过滤原始订单，规范化状态与金额
CREATE OR REPLACE TABLE ${target} AS
SELECT
    order_id,
    store_id,
    product_id,
    CAST(amount AS DOUBLE) AS amount,
    CASE
        WHEN status IN ('paid', 'shipped', 'done') THEN 'valid'
        ELSE status
    END AS order_status,
    ${dt} AS dt
FROM ${table:orders_raw}
WHERE dt = ${dt}
  AND status IN ('paid', 'shipped', 'done')
  AND amount > 0;
