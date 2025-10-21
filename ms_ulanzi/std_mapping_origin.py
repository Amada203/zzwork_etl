# -*- coding: utf-8 -*-
import logging

from dw_util.util import str_utils
from pigeon.connector import new_impala_connector
from pigeon.utils import init_logging
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from spark_util.insert import spark_write_hive

TARGET_DB = '{{ std_mapping_origin }}'.split('.')[0]
TARGET_TABLE = '{{ std_mapping_origin }}'.split('.')[1]
CALC_PARTITION = '{{ month_dt }}'
REFRESH_STATS = False
EMR = True
PARTITION_NUM = 600
COMPRESSION = 'gzip'

update_mode = '{{ update_mode }}'
project_start_month = '{{ project_start_month }}'
unit_weight_threshold = {{ unit_weight_threshold }}

if update_mode == 'incremental':
    month_field = ',month_dt'
    month_condition = f'month_dt = "{CALC_PARTITION}"'
    month_partition = f'month_dt = "{CALC_PARTITION}"'
    drop_incremental_stats = f'DROP INCREMENTAL STATS {TARGET_DB}.{TARGET_TABLE} PARTITION(month_dt="{CALC_PARTITION}");'
elif update_mode == 'full':
    month_field = ',month_dt'
    month_condition = 'month_dt IS NOT NULL'
    month_partition = 'month_dt'
    drop_incremental_stats = f'DROP INCREMENTAL STATS {TARGET_DB}.{TARGET_TABLE};'

DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_DB}.{TARGET_TABLE}(
  unique_id        BIGINT,
  platform         STRING,
  market           STRING,
  item_id          STRING,
  pfsku_id         STRING,
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
  recommend_remark STRING,
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
  pfsku_title_cn   STRING,
  sub_category     STRING,
  sub_category_cn  STRING,
  category_1_cn    STRING,
  category_2_cn    STRING,
  category_3_cn    STRING,
  category_4_cn    STRING,
  category_5_cn    STRING,
  category_6_cn    STRING
) COMMENT '合并标注和attributes属性表'
PARTITIONED BY (month_dt STRING)
STORED AS PARQUET
"""

def spark_sql_with_log(spark, query):
    logging.info(f"执行SQL: {query}")
    return spark.sql(query)

def dumper(spark) -> DataFrame:
    # 构建完整的查询
    query = f"""
    /* 标注数据 */
    SELECT FNV_HASH(concat_ws('|', a.platform, COALESCE(a.market, 'cn'), CAST(a.item_id AS STRING), CAST(a.pfsku_id AS STRING), CAST(1 AS STRING), a.month_dt, CAST(NVL(a.shop_id, '0') AS STRING))) AS unique_id,
           a.platform,
           COALESCE(a.market, 'cn') AS market,
           CAST(a.item_id AS STRING) AS item_id,
           CAST(a.pfsku_id AS STRING) AS pfsku_id,
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
           a.pfsku_title_cn,
           a.sub_category,
           a.sub_category_cn,
           a.category_1_cn,
           a.category_2_cn,
           a.category_3_cn,
           a.category_4_cn,
           a.category_5_cn,
           a.category_6_cn,

           -- SKU计算字段：使用标注结果
           b.sku_no,
           CAST(COUNT(b.sku_no) OVER (PARTITION BY a.month_dt, a.platform, CAST(a.item_id AS STRING), CAST(a.pfsku_id AS STRING), a.shop_id) AS INT) AS sku_num,
           CAST(NULL AS STRING) AS sku_image,
           b.sku_title,
           CAST(NULL AS DOUBLE) AS sku_value_ratio,
           CAST(NULL AS STRING) AS sku_value_ratio_src,
           b.is_bundle,
           b.is_gift,
           b.package,
           b.weight,
           b.package * b.weight AS total_weight,
           '{{ pfsku_package_splited }}' AS total_weight_src,
           CAST(NULL AS STRING) AS attributes,
           '{{ pfsku_package_splited }}' AS sku_src,

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
           CAST(NULL AS STRING) AS std_spu_name_src

           {month_field}
    FROM {{ monthly_basic_info }} a
    LEFT JOIN {{ pfsku_package_splited }} b ON lower(a.platform) = lower(b.platform)
    AND CAST(a.item_id AS STRING) = b.item_id
    AND CAST(a.pfsku_id AS STRING) = b.pfsku_id
    AND a.month_dt = b.month_dt
    AND (a.platform != 'Amazon' OR a.market = b.market)
    LEFT JOIN dw_brand_mapping.final_result c ON c.platform = 'Tmall' 
    AND a.brand_id = CAST(c.brand_id AS STRING)
    AND a.category_1 = CAST(c.category_1 AS STRING)
    WHERE b.item_id IS NOT NULL
    AND {month_condition}

    UNION ALL

    /* 非标注数据，留到下一步清洗 */
    SELECT FNV_HASH(concat_ws('|', a.platform, COALESCE(a.market, 'cn'), CAST(a.item_id AS STRING), CAST(a.pfsku_id AS STRING), CAST(1 AS STRING), a.month_dt, CAST(NVL(a.shop_id, '0') AS STRING))) AS unique_id,
           a.platform,
           COALESCE(a.market, 'cn') AS market,
           CAST(a.item_id AS STRING) AS item_id,
           CAST(a.pfsku_id AS STRING) AS pfsku_id,
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
           a.pfsku_title_cn,
           a.sub_category,
           a.sub_category_cn,
           a.category_1_cn,
           a.category_2_cn,
           a.category_3_cn,
           a.category_4_cn,
           a.category_5_cn,
           a.category_6_cn,

           -- SKU计算字段：使用monthly_attributes逻辑 + GPT重量清洗
           1 AS sku_no,
           CAST(COUNT(1) OVER (PARTITION BY a.month_dt, a.platform, CAST(a.item_id AS STRING), CAST(a.pfsku_id AS STRING), a.shop_id) AS INT) AS sku_num,
           CAST(NULL AS STRING) AS sku_image,
           CASE 
               WHEN a.pfsku_title IS NOT NULL AND a.pfsku_title != '' AND a.pfsku_title != 'null'
               THEN CONCAT(COALESCE(a.item_title, ''), ' ', a.pfsku_title)
               ELSE COALESCE(a.item_title, '')
           END AS sku_title,
           CAST(NULL AS DOUBLE) AS sku_value_ratio,
           CAST(NULL AS STRING) AS sku_value_ratio_src,
           0 AS is_bundle,
           0 AS is_gift,
           -- 重量字段：优先使用GPT清洗结果，否则使用monthly_attributes逻辑（当前为NULL）
           ifnull(d.package_all, 1) as package,
           -- 使用「单件重量」限制
           if(ifnull(d.weight_all_in_g, 0) / ifnull(d.package_all, 1) < {unit_weight_threshold}, ifnull(d.weight_all_in_g, 0) / ifnull(d.package_all, 1), null) as weight,
           if(ifnull(d.weight_all_in_g, 0) / ifnull(d.package_all, 1) < {unit_weight_threshold}, ifnull(d.weight_all_in_g, 0), null) as total_weight,
           if(ifnull(d.weight_all_in_g, 0) / ifnull(d.package_all, 1) < {unit_weight_threshold} OR d.weight_all_in_g IS NULL, IF(d.weight_all_in_g IS NOT NULL, 'GPT克重清洗', '机器清洗'), "超过阈值{unit_weight_threshold}g") as total_weight_src,
           CAST(NULL AS STRING) AS attributes,
           '机器清洗' AS sku_src,

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
           CAST(NULL AS STRING) AS std_spu_name_src

           {month_field}
    FROM {{ monthly_basic_info }} a LEFT anti
    JOIN {{ pfsku_package_splited }} b ON lower(a.platform) = lower(b.platform)
    AND CAST(a.item_id AS STRING) = b.item_id
    AND CAST(a.pfsku_id AS STRING) = b.pfsku_id
    AND a.month_dt = b.month_dt
    AND (a.platform != 'Amazon' OR a.market = b.market)
    LEFT JOIN dw_brand_mapping.final_result c ON c.platform = 'Tmall' 
    AND a.brand_id = CAST(c.brand_id AS STRING)
    AND a.category_1 = CAST(c.category_1 AS STRING)
    LEFT JOIN {{ datahub_dwd_gpt_weight_info_monthly }} d ON lower(a.platform) = lower(d.platform)
    AND CAST(a.item_id AS STRING) = CAST(d.product_id AS STRING)
    AND CAST(a.pfsku_id AS STRING) = CAST(d.sku_id AS STRING)
    AND a.month_dt = d.month_dt  
    WHERE {month_condition}
    """
    
    # 执行查询并返回 DataFrame
    df = spark_sql_with_log(spark, query)
    return df


def loader(spark, df: DataFrame):
    spark_write_hive(df, spark, TARGET_DB, TARGET_TABLE, create_table_ddl=DDL, dynamic_partition='month_dt', emr=EMR,
                     refresh_stats=REFRESH_STATS, compression=COMPRESSION, repartition_num=PARTITION_NUM)


def main():
    init_logging()
    impala = new_impala_connector(emr=True)
    spark = (SparkSession.builder.appName("rearc.{{dag_name}}.{{job_name}}.{{execution_date}}")
             .enableHiveSupport().getOrCreate())
    spark.sql(f'set spark.sql.shuffle.partitions={PARTITION_NUM}')
    spark.sql(f'set spark.sql.autoBroadcastJoinThreshold=-1')
    spark.sql(f'set spark.sql.broadcastTimeout=600')

    df = dumper(spark)
    loader(spark, df)
    
    spark.stop()
    impala.execute(f'REFRESH {TARGET_DB}.{TARGET_TABLE} PARTITION({month_partition})')
    impala.execute(drop_incremental_stats)
    impala.execute(f'COMPUTE INCREMENTAL STATS {TARGET_DB}.{TARGET_TABLE} PARTITION({month_partition})')


if __name__ == '__main__':
    main()