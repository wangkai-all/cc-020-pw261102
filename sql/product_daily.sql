-- 独立分支：日商品统计，只依赖源数据，不受主链路失败影响
CREATE OR REPLACE TABLE ${target} AS
SELECT
    product_id,
    COUNT(*)    AS sold_cnt,
    SUM(amount) AS sales_amount,
    ${dt} AS dt
FROM ${table:orders_raw}
WHERE dt = ${dt}
  AND status IN ('paid', 'shipped', 'done')
  AND amount > 0
GROUP BY product_id;
