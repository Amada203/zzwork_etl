set DECIMAL_V2=false;
CREATE TABLE IF NOT EXISTS {{ amazon_monthly_price }} (
    item_id                      STRING,
    pfsku_id                     STRING,
    page_price                   DOUBLE,
    discount_price               DOUBLE,
    discount_price_src           STRING,
    market                       STRING
) PARTITIONED BY (month_dt STRING) 
COMMENT 'Amazon月度价格表' 
STORED AS PARQUET;


INSERT OVERWRITE {{ amazon_monthly_price }} PARTITION(month_dt='{{ month_dt }}')
/* +NOSHUFFLE,NOCLUSTERED */
SELECT 
    item_id,                    -- 商品ID
    pfsku_id,                  -- 商品SKU ID
    page_price,                -- 商品SKU月度券后价格
    discount_price,            -- SKU月度页面价格
    '月销表价格' AS discount_price_src,  -- 到手价格来源
    market                     -- 地区
FROM {{ amazon_monthly_sales }}
WHERE month_dt = '{{ month_dt }}'
  AND pfsku_id IS NOT NULL
  AND market IS NOT NULL
;


-- 合并小文件
SET NUM_NODES = 2;
-- 表比较大，需要压缩
SET COMPRESSION_CODEC=GZIP;

INSERT OVERWRITE {{ amazon_monthly_price }} PARTITION(month_dt='{{ month_dt }}')
/* +NOSHUFFLE,NOCLUSTERED */
SELECT 
    item_id,
    pfsku_id,
    page_price,
    discount_price,
    discount_price_src,
    market
FROM {{ amazon_monthly_price }} 
WHERE month_dt='{{ month_dt }}';


-- {{ refresh }} {{ amazon_monthly_price }};
DROP INCREMENTAL STATS {{ amazon_monthly_price }} PARTITION (month_dt='{{ month_dt }}');
COMPUTE INCREMENTAL STATS {{ amazon_monthly_price }};
