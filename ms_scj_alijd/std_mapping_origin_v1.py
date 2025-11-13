set DECIMAL_V2=false;
{% macro clean(month_field, month_condition, month_partition, drop_incremental_stats) -%}

CREATE TABLE IF NOT EXISTS {{ std_mapping_origin }}(
  unique_id        BIGINT,
  platform         STRING,
  market           STRING,
  item_id          BIGINT,
  pfsku_id         BIGINT,
  item_title       STRING,
  pfsku_title      STRING,
  category_1       STRING,
  category_2       STRING,
  category_3       STRING,
  category_4       STRING,
  category_5       STRING,
  category_6       STRING,
  category_1_id    STRING,
  category_2_id    STRING,
  category_3_id    STRING,
  category_4_id    STRING,
  category_5_id    STRING,
  category_6_id    STRING,
  category_id      BIGINT,
  category_name    STRING,
  shop_id          STRING,
  shop_name        STRING,
  unique_shop_name STRING,
  shop_type        STRING,
  brand_id         STRING,
  brand_name       STRING,
  ai_brand_name    STRING,
  properties       STRING,
  shop_url         STRING,
  item_url         STRING,
  pfsku_url        STRING,
  item_image       STRING,
  item_images      STRING,
  pfsku_image      STRING,
  tags             STRING,
  basic_info       STRING,
  recommend_remark         STRING,
  sku_no                   INT,
  sku_num                  INT,
  sku_image                STRING,
  sku_title                STRING,
  sku_value_ratio          DOUBLE,
  sku_value_ratio_src      STRING,
  is_bundle                INT,
  is_gift                  INT,
  package                  INT,
  weight                   DOUBLE,
  total_weight             DOUBLE,
  total_weight_src         STRING,
  attributes               STRING,
  sku_src                  STRING,
  media                            STRING,
  std_category_name                STRING,
  std_sub_category_name            STRING,
  std_brand_name                   STRING,
  manufacturer                     STRING,
  variant                          STRING,
  std_spu_name                     STRING,
  std_sku_name                     STRING,
  media_src                        STRING,
  std_category_name_src            STRING,
  std_sub_category_name_src        STRING,
  std_brand_name_src               STRING,
  manufacturer_src                 STRING,
  variant_src                      STRING,
  std_spu_name_src                 STRING,
  std_sub_segment                  STRING,
  std_sub_segment_src              STRING,
  quantity                         DOUBLE,
  form                             STRING,
  form_src                         STRING,
  target_format                    STRING,
  target_format_src                STRING
) PARTITIONED BY (month_dt STRING)
COMMENT '合并标注和attributes属性表'
STORED AS PARQUET;


INSERT OVERWRITE {{ std_mapping_origin }} PARTITION({{ month_partition }})
/* +NOSHUFFLE,NOCLUSTERED */
/* 标注数据 */
SELECT FNV_HASH(concat_ws('|', a.platform, 'cn', CAST(a.item_id AS STRING), CAST(a.pfsku_id AS STRING), CAST(b.sku_no AS STRING), a.month_dt, CAST(NVL(a.shop_id, '0') AS STRING))) AS unique_id,
       a.platform,
       'cn' AS market,
       a.item_id,
       a.pfsku_id,
       a.item_title,
       a.pfsku_title,
       a.category_1,
       a.category_2,
       a.category_3,
       a.category_4,
       a.category_5,
       a.category_6,
       a.category_1_id,
       a.category_2_id,
       a.category_3_id,
       a.category_4_id,
       a.category_5_id,
       a.category_6_id,
       a.category_id,
       a.category_name,
       a.shop_id,
       a.shop_name,
       CAST(NULL AS STRING) AS unique_shop_name,
       a.shop_type,
       a.brand_id,
       a.brand_name,
       c.std_brand_name AS ai_brand_name,
       a.properties,
       a.shop_url,
       a.item_url,
       a.pfsku_url,
       a.item_image,
       a.item_images,
       a.pfsku_image,
       a.tags,
       a.basic_info,
       a.recommend_remark,

       b.sku_no,
       CAST(COUNT(b.sku_no) OVER (PARTITION BY a.month_dt, a.platform, a.item_id, a.pfsku_id, a.shop_id) AS INT) AS sku_num,
       CAST(NULL AS STRING) AS sku_image,
       -- CASE WHEN CAST(COUNT(b.sku_no) OVER (PARTITION BY a.month_dt, a.platform, a.item_id, a.pfsku_id, a.shop_id) AS INT) = 1 THEN concat(b.std_spu_name,ifnull(b.weight,''),if(b.weight is null,'',b.weight_unit),if(b.weight is null,''cast(b.package as string))) ELSE concat(""（套组拆分）"",b.std_spu_name,ifnull(b.weight,''),if(b.weight is null,'',b.weight_unit),if(b.weight is null,''cast(b.package as string))) END AS sku_title, --20251113modify 原逻辑
       CASE
           WHEN CAST(COUNT(b.sku_no) OVER (PARTITION BY a.month_dt, a.platform, a.item_id, a.pfsku_id, a.shop_id) AS INT) = 1 THEN concat(
               b.std_spu_name,
               COALESCE(CAST(b.weight AS STRING), CAST(a.weight AS STRING), ''),
               IF(COALESCE(b.weight, a.weight) IS NULL, '', b.weight_unit),
               IF(COALESCE(b.weight, a.weight) IS NULL, '', CAST(COALESCE(b.package_num, a.package) AS STRING))
           )
           ELSE concat(
               "（套组拆分）",
               b.std_spu_name,
               COALESCE(CAST(b.weight AS STRING), CAST(a.weight AS STRING), ''),
               IF(COALESCE(b.weight, a.weight) IS NULL, '', b.weight_unit),
               IF(COALESCE(b.weight, a.weight) IS NULL, '', CAST(COALESCE(b.package_num, a.package) AS STRING))
           )
       END AS sku_title, --20251113modify 新逻辑
       CAST(NULL AS DOUBLE) AS sku_value_ratio,
       CAST(NULL AS STRING) AS sku_value_ratio_src,
       b.is_bundle,
       b.is_gift,
       COALESCE(b.package_num, a.package) AS package,
       COALESCE(b.weight, a.weight) AS weight,
       COALESCE(b.weight, a.weight) * COALESCE(b.package_num, a.package) AS total_weight,
       -- case when b.package_num is not null then '{{ pfsku_package_splited }}' else a.total_weight_src end AS total_weight_src,
       IF(b.weight IS NULL, '机器清洗', 'ms_scj_alijd.scj_pfsku_package_splited') AS total_weight_src,
       CAST(NULL AS STRING) AS attributes,
       '{{ pfsku_package_splited }}' AS sku_src,

       -- 完全走 OneMap
       CAST(NULL AS STRING) AS media,
       CAST(NULL AS STRING) AS std_category_name,
       CAST(NULL AS STRING) AS std_sub_category_name,
       ifnull(b.std_brand_name,a.brand_name) std_brand_name,
       CAST(NULL AS STRING) AS manufacturer,
       CAST(NULL AS STRING) AS variant,
       -- concat(b.std_spu_name, ifnull(b.flavor,'')) AS std_spu_name,
       b.std_spu_name AS std_spu_name, --20251113modify
       -- CASE WHEN CAST(COUNT(b.sku_no) OVER (PARTITION BY a.month_dt, a.platform, a.item_id, a.pfsku_id, a.shop_id) AS INT) = 1 THEN concat(b.std_spu_name,ifnull(b.weight,''),if(b.weight is null,'',b.weight_unit),if(b.weight is null,''cast(b.package as string))) ELSE concat(""（套组拆分）"",b.std_spu_name,ifnull(b.weight,''),if(b.weight is null,'',b.weight_unit),if(b.weight is null,''cast(b.package as string))) END  AS std_sku_name,   --20251113modify 原逻辑
       CASE
           WHEN CAST(COUNT(b.sku_no) OVER (PARTITION BY a.month_dt, a.platform, a.item_id, a.pfsku_id, a.shop_id) AS INT) = 1 THEN 
           concat(
               b.std_spu_name,
               COALESCE(CAST(b.weight AS STRING), CAST(a.weight AS STRING), ''),
               IF(COALESCE(b.weight, a.weight) IS NULL, '', b.weight_unit),
               IF(COALESCE(b.weight, a.weight) IS NULL, '', CAST(COALESCE(b.package_num, a.package) AS STRING))
           )
           ELSE concat(
               "（套组拆分）",
               b.std_spu_name,
               COALESCE(CAST(b.weight AS STRING), CAST(a.weight AS STRING), ''),
               IF(COALESCE(b.weight, a.weight) IS NULL, '', b.weight_unit),
               IF(COALESCE(b.weight, a.weight) IS NULL, '', CAST(COALESCE(b.package_num, a.package) AS STRING))
           )
       END  AS std_sku_name,   --20251113modify 新逻辑
       CAST(NULL AS STRING) AS media_src,
       CAST(NULL AS STRING) AS std_category_name_src,
       CAST(NULL AS STRING) AS std_sub_category_name_src,
       if(b.std_brand_name is null, '机器清洗', '{{ pfsku_package_splited }}') AS std_brand_name_src,
       CAST(NULL AS STRING) AS manufacturer_src,
       CAST(NULL AS STRING) AS variant_src,
       '{{ pfsku_package_splited }}' AS std_spu_name_src,
       b.std_category_3 AS std_sub_segment,
       '{{ pfsku_package_splited }}' AS std_sub_segment_src,
       b.quantity AS quantity,
       b.form AS form,
       '{{ pfsku_package_splited }}' AS form_src,
       b.target_format AS target_format,
       '{{ pfsku_package_splited }}' AS target_format_src

       {{ month_field }}
FROM {{ monthly_attributes }} a
LEFT JOIN {{ pfsku_package_splited }} b ON lower(a.platform) = lower(b.platform)
AND a.item_id = b.item_id
AND a.pfsku_id = b.pfsku_id
AND a.month_dt = b.month_dt
LEFT JOIN dw_brand_mapping.final_result c ON c.platform = 'Tmall' 
AND a.brand_id = CAST(c.brand_id AS STRING)
AND a.category_1 = CAST(c.category_1 AS STRING)
WHERE b.item_id IS NOT NULL
AND {{ month_condition }}

UNION ALL

/* 非标注数据，留到下一步清洗 */
SELECT FNV_HASH(concat_ws('|', a.platform, 'cn', CAST(a.item_id AS STRING), CAST(a.pfsku_id AS STRING), CAST(a.sku_no AS STRING), a.month_dt, CAST(NVL(a.shop_id, '0') AS STRING))) AS unique_id,
       a.platform,
       'cn' AS market,
       a.item_id,
       a.pfsku_id,
       a.item_title,
       a.pfsku_title,
       a.category_1,
       a.category_2,
       a.category_3,
       a.category_4,
       a.category_5,
       a.category_6,
       a.category_1_id,
       a.category_2_id,
       a.category_3_id,
       a.category_4_id,
       a.category_5_id,
       a.category_6_id,
       a.category_id,
       a.category_name,
       a.shop_id,
       a.shop_name,
       CAST(NULL AS STRING) AS unique_shop_name,
       a.shop_type,
       a.brand_id,
       a.brand_name,
       c.std_brand_name AS ai_brand_name,
       a.properties,
       a.shop_url,
       a.item_url,
       a.pfsku_url,
       a.item_image,
       a.item_images,
       a.pfsku_image,
       a.tags,
       a.basic_info,
       a.recommend_remark,

       a.sku_no,
       CAST(COUNT(a.sku_no) OVER (PARTITION BY a.month_dt, a.platform, a.item_id, a.pfsku_id, a.shop_id) AS INT) AS sku_num,
       CAST(NULL AS STRING) AS sku_image,
       a.sku_title,
       CAST(NULL AS DOUBLE) AS sku_value_ratio,
       CAST(NULL AS STRING) AS sku_value_ratio_src,
       a.is_bundle,
       0 AS is_gift,
       ifnull(d.package_all, ifnull(a.package, 1)) as package,
       -- 使用「单件重量」限制
       if(ifnull(d.weight_all_in_g, a.total_weight) / ifnull(d.package_all, ifnull(a.package, 1)) < {{ unit_weight_threshold }}, ifnull(d.weight_all_in_g, a.total_weight) / ifnull(d.package_all, ifnull(a.package, 1)), null) as weight,
       if(ifnull(d.weight_all_in_g, a.total_weight) / ifnull(d.package_all, ifnull(a.package, 1)) < {{ unit_weight_threshold }}, ifnull(d.weight_all_in_g, a.total_weight), null) as total_weight,
       if(ifnull(d.weight_all_in_g, a.total_weight) / ifnull(d.package_all, ifnull(a.package, 1)) < {{ unit_weight_threshold }} OR a.total_weight_src IS NULL, IF(d.weight_all_in_g IS NOT NULL, 'GPT克重清洗', a.total_weight_src), "超过阈值{{ unit_weight_threshold }}g") as total_weight_src,
       -- 使用「总重」限制
       -- if(a.total_weight / ifnull(a.package, 1) > 0 and a.total_weight < {{ max_total_weight }}, a.total_weight / ifnull(a.package, 1), null) as weight,
       -- if(a.total_weight / ifnull(a.package, 1) > 0 and a.total_weight < {{ max_total_weight }}, a.total_weight, null) as total_weight,
       -- if(a.total_weight / ifnull(a.package, 1) > 0 and a.total_weight < {{ max_total_weight }} OR a.total_weight_src IS NULL, a.total_weight_src, "超过阈值{{ max_total_weight }}g") as total_weight_src,
       a.attributes,
       a.sku_src,

       -- 完全走 OneMap
       CAST(NULL AS STRING) AS media,
       CAST(NULL AS STRING) AS std_category_name,
       CAST(NULL AS STRING) AS std_sub_category_name,
       CAST(NULL AS STRING) AS std_brand_name,
       CAST(NULL AS STRING) AS manufacturer,
       CAST(NULL AS STRING) AS variant,
       CAST(NULL AS STRING) AS std_spu_name,
       CAST(NULL AS STRING) AS std_sku_name,

       CAST(NULL AS STRING) AS media_src,
       CAST(NULL AS STRING) AS std_category_name_src,
       CAST(NULL AS STRING) AS std_sub_category_name_src,
       CAST(NULL AS STRING) AS std_brand_name_src,
       CAST(NULL AS STRING) AS manufacturer_src,
       CAST(NULL AS STRING) AS variant_src,
       CAST(NULL AS STRING) AS std_spu_name_src,
       CAST(NULL AS STRING) AS std_sub_segment,
       CAST(NULL AS STRING) AS std_sub_segment_src,
       CAST(NULL AS DOUBLE) AS quantity,
       CAST(NULL AS STRING) AS form,
       CAST(NULL AS STRING) AS form_src,
       CAST(NULL AS STRING) AS target_format,
       CAST(NULL AS STRING) AS target_format_src

       {{ month_field }}
FROM {{ monthly_attributes }} a LEFT anti
JOIN {{ pfsku_package_splited }} b ON lower(a.platform) = lower(b.platform)
AND a.item_id = b.item_id
AND a.pfsku_id = b.pfsku_id
AND a.month_dt = b.month_dt
LEFT JOIN dw_brand_mapping.final_result c ON c.platform = 'Tmall' 
AND a.brand_id = CAST(c.brand_id AS STRING)
AND a.category_1 = CAST(c.category_1 AS STRING)
LEFT JOIN {{ datahub_dwd_gpt_weight_info_monthly }} d ON lower(a.platform) = lower(d.platform)
AND a.item_id =  CAST(d.product_id AS BIGINT)
AND a.pfsku_id = CAST(d.sku_id AS BIGINT)
AND a.month_dt = d.month_dt  
WHERE {{ month_condition }}
;

-- 合并小文件
SET NUM_NODES = 3;
INSERT OVERWRITE {{ std_mapping_origin }} PARTITION({{ month_partition }})
/* +NOSHUFFLE,NOCLUSTERED */
SELECT unique_id,
       platform,
       market,
       item_id,
       pfsku_id,
       item_title,
       pfsku_title,
       category_1,
       category_2,
       category_3,
       category_4,
       category_5,
       category_6,
       category_1_id,
       category_2_id,
       category_3_id,
       category_4_id,
       category_5_id,
       category_6_id,
       category_id,
       category_name,
       shop_id,
       shop_name,
       unique_shop_name,
       shop_type,
       brand_id,
       brand_name,
       ai_brand_name,
       properties,
       shop_url,
       item_url,
       pfsku_url,
       item_image,
       item_images,
       pfsku_image,
       tags,
       basic_info,
       recommend_remark,
       sku_no,
       sku_num,
       sku_image,
       sku_title,
       sku_value_ratio,
       sku_value_ratio_src,
       is_bundle,
       is_gift,
       `package`,
       weight,
       total_weight,
       total_weight_src,
       attributes,
       sku_src,
       media,
       std_category_name,
       std_sub_category_name,
       std_brand_name,
       manufacturer,
       variant,
       std_spu_name,
       std_sku_name,
       media_src,
       std_category_name_src,
       std_sub_category_name_src,
       std_brand_name_src,
       manufacturer_src,
       variant_src,
       std_spu_name_src,
       std_sub_segment,
       std_sub_segment_src,
       quantity,
       form,
       form_src,
       target_format,
       target_format_src
       {{ month_field }}
FROM {{ std_mapping_origin }} a
WHERE {{ month_condition }};


-- INVALIDATE METADATA {{ std_mapping_origin }};
{{ drop_incremental_stats }}
COMPUTE INCREMENTAL STATS {{ std_mapping_origin }};

{%- endmacro %}


{% if update_mode == 'incremental' %}
-- 增量更新 {{ month_dt }}
{{ clean(month_field = '',
         month_condition = 'a.month_dt = "' + month_dt + '"',
         month_partition = 'month_dt = "' + month_dt + '"',
         drop_incremental_stats = 'DROP INCREMENTAL STATS ' + std_mapping_origin + ' PARTITION(month_dt="' + month_dt + '");') }}

{% elif update_mode == 'full' %}
-- 全量更新
{{ clean(month_field = ',a.month_dt',
         month_condition = '1=1',
         month_partition = 'month_dt',
         drop_incremental_stats = 'DROP INCREMENTAL STATS ' + std_mapping_origin + ' PARTITION(month_dt>="' + project_start_month + '");') }}
{% endif %}

{#
-- 若项目不使用 unique_shop_name 字段，则无需启用该逻辑
INSERT OVERWRITE TABLE {{ std_mapping_origin }} PARTITION(month_dt)
/* +NOSHUFFLE,NOCLUSTERED */
SELECT a.unique_id,
       a.platform,
       a.market,
       a.item_id,
       a.pfsku_id,
       a.item_title,
       a.pfsku_title,
       a.category_1,
       a.category_2,
       a.category_3,
       a.category_4,
       a.category_5,
       a.category_6,
       a.category_1_id,
       a.category_2_id,
       a.category_3_id,
       a.category_4_id,
       a.category_5_id,
       a.category_6_id,
       a.category_id,
       a.category_name,
       a.shop_id,
       a.shop_name,
       b.unique_shop_name,
       a.shop_type,
       a.brand_id,
       a.brand_name,
       a.ai_brand_name,
       a.properties,
       a.shop_url,
       a.item_url,
       a.pfsku_url,
       a.item_image,
       a.item_images,
       a.pfsku_image,
       a.tags,
       a.basic_info,
       a.recommend_remark,
       a.sku_no,
       a.sku_num,
       a.sku_image,
       a.sku_title,
       a.sku_value_ratio,
       a.sku_value_ratio_src,
       a.is_bundle,
       a.is_gift,
       a.`package`,
       a.weight,
       a.total_weight,
       a.total_weight_src,
       a.attributes,
       a.sku_src,
       a.media,
       a.std_category_name,
       a.std_sub_category_name,
       a.std_brand_name,
       a.manufacturer,
       a.variant,
       a.std_spu_name,
       a.std_sku_name,
       a.media_src,
       a.std_category_name_src,
       a.std_sub_category_name_src,
       a.std_brand_name_src,
       a.manufacturer_src,
       a.variant_src,
       a.std_spu_name_src,
       a.month_dt
FROM {{ std_mapping_origin }} a 
LEFT JOIN {{ z_shop_name }} b ON a.platform = b.platform
AND a.market = b.market
AND a.shop_id = b.shop_id
AND a.month_dt = b.month_dt
;


{{ refresh }} {{ std_mapping_origin }};
COMPUTE INCREMENTAL STATS {{ std_mapping_origin }};
#}