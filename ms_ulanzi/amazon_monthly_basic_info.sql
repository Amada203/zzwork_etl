set DECIMAL_V2=false;
CREATE TABLE IF NOT EXISTS {{ amazon_monthly_basic_info }} (
    item_id                      STRING,
    pfsku_id                     STRING,
    pfsku_title                  STRING,
    pfsku_title_cn               STRING,
    pfsku_url                    STRING,
    sub_category                 STRING,
    sub_category_cn              STRING,
    category_name                STRING,
    category_1                   STRING,
    category_2                   STRING,
    category_3                   STRING,
    category_4                   STRING,
    category_5                   STRING,
    category_1_cn                STRING,
    category_2_cn                STRING,
    category_3_cn                STRING,
    category_4_cn                STRING,
    category_5_cn                STRING,
    shop_id                      STRING,
    shop_name                    STRING,
    manufacturer                 STRING,
    brand_name                   STRING,
    pfsku_image                  STRING,
    market                       STRING
) PARTITIONED BY (month_dt STRING) 
COMMENT 'Amazon月度基础信息表' 
STORED AS PARQUET;


INSERT OVERWRITE {{ amazon_monthly_basic_info }} PARTITION(month_dt='{{ month_dt }}')
/* +NOSHUFFLE,NOCLUSTERED */
SELECT 
    item_id,                    -- 商品ID
    pfsku_id,                  -- 商品SKU ID
    pfsku_title,               -- 商品名称
    pfsku_title_cn,            -- 商品名称_中文
    pfsku_url,                 -- 商品连接
    sub_category,               -- 子类目
    sub_category_cn,            -- 子类目_中文
    category_name,              -- 末级品类名
    category_1,                 -- 一级品类名
    category_2,                 -- 二级品类名
    category_3,                 -- 三级品类名
    category_4,                 -- 四级品类名
    category_5,                 -- 五级品类名
    category_1_cn,              -- 一级品类名_中文
    category_2_cn,              -- 二级品类名_中文
    category_3_cn,              -- 三级品类名_中文
    category_4_cn,              -- 四级品类名_中文
    category_5_cn,              -- 五级品类名_中文
    shop_id,                    -- 店铺ID
    shop_name,                  -- 店铺名称
    manufacturer,               -- 制造商
    brand_name,                 -- 品牌名称
    pfsku_image,                -- 平台商品SKU图片
    market                      -- 地区
FROM {{ amazon_monthly_sales }}
WHERE month_dt = '{{ month_dt }}'
  AND pfsku_id IS NOT NULL
  AND market IS NOT NULL
;


-- 合并小文件
SET NUM_NODES = 2;
-- 表比较大，需要压缩
SET COMPRESSION_CODEC=GZIP;

INSERT OVERWRITE {{ amazon_monthly_basic_info }} PARTITION(month_dt='{{ month_dt }}')
/* +NOSHUFFLE,NOCLUSTERED */
SELECT 
    item_id,
    pfsku_id,
    pfsku_title,
    pfsku_title_cn,
    pfsku_url,
    sub_category,
    sub_category_cn,
    category_name,
    category_1,
    category_2,
    category_3,
    category_4,
    category_5,
    category_1_cn,
    category_2_cn,
    category_3_cn,
    category_4_cn,
    category_5_cn,
    shop_id,
    shop_name,
    manufacturer,
    brand_name,
    pfsku_image,
    market
FROM {{ amazon_monthly_basic_info }} 
WHERE month_dt='{{ month_dt }}';


-- {{ refresh }} {{ amazon_monthly_basic_info }};
DROP INCREMENTAL STATS {{ amazon_monthly_basic_info }} PARTITION (month_dt='{{ month_dt }}');
COMPUTE INCREMENTAL STATS {{ amazon_monthly_basic_info }};
