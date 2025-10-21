set DECIMAL_V2=false;
CREATE TABLE IF NOT EXISTS {{ amazon_monthly_count }} (
    item_id                      STRING,
    pfsku_id                     STRING,
    pfsku_unit_sales            BIGINT,
    market                       STRING
) PARTITIONED BY (month_dt STRING) 
COMMENT 'Amazon月度销量表' 
STORED AS PARQUET;


INSERT OVERWRITE {{ amazon_monthly_count }} PARTITION(month_dt='{{ month_dt }}')
/* +NOSHUFFLE,NOCLUSTERED */
SELECT 
    item_id,                    -- 商品ID
    pfsku_id,                  -- 商品SKU ID
    pfsku_unit_sales,          -- 商品SKU月度销售件数
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

INSERT OVERWRITE {{ amazon_monthly_count }} PARTITION(month_dt='{{ month_dt }}')
/* +NOSHUFFLE,NOCLUSTERED */
SELECT 
    item_id,
    pfsku_id,
    pfsku_unit_sales,
    market
FROM {{ amazon_monthly_count }} 
WHERE month_dt='{{ month_dt }}';


-- {{ refresh }} {{ amazon_monthly_count }};
DROP INCREMENTAL STATS {{ amazon_monthly_count }} PARTITION (month_dt='{{ month_dt }}');
COMPUTE INCREMENTAL STATS {{ amazon_monthly_count }};
