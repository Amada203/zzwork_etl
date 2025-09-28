{% macro clean(month_field, month_condition, month_partition, drop_incremental_stats) -%}

CREATE TABLE IF NOT EXISTS {{ feigua_monthly_sales }} (
    platform STRING COMMENT '平台',
    item_id STRING COMMENT '商品ID',
    sales DOUBLE COMMENT '商品销售额',
    count BIGINT COMMENT '商品销量',
    price DOUBLE COMMENT '商品销售价格',
    product_url STRING COMMENT '商品链接',
    product_name STRING COMMENT '商品标题',
    shop_id STRING COMMENT '店铺ID',
    shop_name STRING COMMENT '店铺名',
    brand_name STRING COMMENT '品牌名'
) PARTITIONED BY (month_dt STRING) 
STORED AS PARQUET;

SET NUM_NODES = 1;
INSERT OVERWRITE TABLE {{ feigua_monthly_sales }} PARTITION({{ month_partition }}) 
SELECT 
    'douyin' as platform,  -- 取固定值douyin
    product_id as item_id,  -- 商品ID
    sales,  -- 商品销售额
    count,  -- 商品销量
    asp as price,  -- 商品销售价格(asp)
    product_url,  -- 商品链接
    product_name,  -- 商品标题
    shop_id,  -- 店铺ID
    shop_name,  -- 店铺名
    brand_name
    {{ month_field }}
FROM  datahub.dws_feigua_monthly_sales_dev_20250922
WHERE asp > 0  
  AND month_dt BETWEEN '2023-01-01' AND '2025-07-01'  -- 数据范围限制
  AND {{ month_condition }};

INVALIDATE METADATA {{ feigua_monthly_sales }};
{{ drop_incremental_stats }}
COMPUTE INCREMENTAL STATS {{ feigua_monthly_sales }};

{%- endmacro %}

{% if update_mode == 'incremental' %}
-- 增量更新 {{ month_dt }}
{{ clean(month_field = '',
         month_condition = 'month_dt = "' + month_dt + '"',
         month_partition = 'month_dt = "' + month_dt + '"',
         drop_incremental_stats = 'DROP INCREMENTAL STATS ' + feigua_monthly_sales + ' PARTITION(month_dt="' + month_dt + '");') }}

{% elif update_mode == 'full' %}
-- 全量更新
{{ clean(month_field = '',
         month_condition = '1=1',
         month_partition =  'month_dt',
         drop_incremental_stats = 'DROP INCREMENTAL STATS ' + feigua_monthly_sales + ' PARTITION(month_dt>="' + project_start_month + '");') }}
{% endif %}
