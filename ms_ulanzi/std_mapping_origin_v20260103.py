# -*- coding: utf-8 -*-
# 多分区增量全量刷数版本 - 20260103
import logging

from pigeon.connector import new_impala_connector
from pigeon.utils import init_logging
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import concat_ws, xxhash64
from spark_util.insert import spark_write_hive

TARGET_DB = '{{ std_mapping_origin }}'.split('.')[0]
TARGET_TABLE = '{{ std_mapping_origin }}'.split('.')[1]
CALC_PARTITION = ['{{ month_dt }}', '{{ platform_filter }}', '{{ market_filter }}']
REFRESH_STATS = False
EMR = True
PARTITION_NUM = 600
COMPRESSION = 'gzip'

update_mode = '{{ update_mode }}'
project_start_month = '{{ project_start_month }}'
unit_weight_threshold = {{ unit_weight_threshold }}

DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_DB}.{TARGET_TABLE}(
  unique_id        BIGINT,
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
) 
PARTITIONED BY (platform STRING, month_dt STRING, market STRING)
STORED AS PARQUET
"""


def _normalize_filter(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value or value.upper() in ('*', 'ALL', 'NULL', 'NONE'):
            return None
        return value
    return value


def spark_sql_with_log(spark, query):
    return spark.sql(query)


def dumper(spark, calc_partition, update_mode, project_start_month):
    if isinstance(calc_partition, list):
        month_dt = _normalize_filter(calc_partition[0] if len(calc_partition) > 0 else None)
        platform_filter = _normalize_filter(calc_partition[1] if len(calc_partition) > 1 else None)
        market_filter = _normalize_filter(calc_partition[2] if len(calc_partition) > 2 else None)
    else:
        month_dt = _normalize_filter(calc_partition)
        platform_filter = None
        market_filter = None
    
    where_conditions = []
    partition_parts = []
    
    # 构建 WHERE 条件和分区条件
    if update_mode == 'incremental':
        if month_dt:
            where_conditions.append(f'a.month_dt = "{month_dt}"')
        if platform_filter:
            where_conditions.append(f'a.platform = "{platform_filter}"')
        if market_filter:
            where_conditions.append(f'a.market = "{market_filter}"')
    elif update_mode == 'full':
        if month_dt:
            where_conditions.append(f'a.month_dt >= "{project_start_month}"')
        else:
            where_conditions.append('a.month_dt IS NOT NULL')
        if platform_filter:
            where_conditions.append(f'a.platform = "{platform_filter}"')
        if market_filter:
            where_conditions.append(f'a.market = "{market_filter}"')
    
    # 构建分区条件（仅包含有值的分区字段）
    if platform_filter:
        partition_parts.append(f'platform = "{platform_filter}"')
    if month_dt:
        partition_parts.append(f'month_dt = "{month_dt}"')
    if market_filter:
        partition_parts.append(f'market = "{market_filter}"')
    
    month_partition = ','.join(partition_parts) if partition_parts else 'platform,month_dt,market'
    drop_incremental_stats = f'DROP INCREMENTAL STATS {TARGET_DB}.{TARGET_TABLE} PARTITION({month_partition});'
    
    month_condition = ' AND '.join(where_conditions) if where_conditions else '1=1'
    
    query = f"""
    SELECT CAST(xxhash64(concat_ws('||', a.platform, COALESCE(a.market, 'cn'), CAST(a.item_id AS STRING), CAST(a.pfsku_id AS STRING), '1', a.month_dt, CAST(NVL(a.shop_id, '0') AS STRING))) AS BIGINT) AS unique_id,
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
           coalesce(d.std_brand_name, a.brand_name)AS ai_brand_name,
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
           CAST(COUNT(b.sku_no) OVER (PARTITION BY a.month_dt, a.platform, CAST(a.item_id AS STRING), CAST(a.pfsku_id AS STRING), a.shop_id,a.market) AS INT) AS sku_num,
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
           CAST(NULL AS STRING) AS media,
           CAST(NULL AS STRING) AS std_category_name,
           CAST(NULL AS STRING) AS std_sub_category_name,
           CAST(NULL AS STRING) AS std_brand_name,
           a.manufacturer AS manufacturer,
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
           a.pfsku_title_cn,
           a.sub_category,
           a.sub_category_cn,
           a.category_1_cn,
           a.category_2_cn,
           a.category_3_cn,
           a.category_4_cn,
           a.category_5_cn,
           a.category_6_cn,
           a.platform,
           a.month_dt,
           COALESCE(a.market, 'cn') AS market
    FROM {{ monthly_basic_info }} a
    LEFT JOIN {{ pfsku_package_splited }} b ON lower(a.platform) = lower(b.platform)
    AND CAST(a.item_id AS STRING) = b.item_id
    AND CAST(a.pfsku_id AS STRING) = b.pfsku_id
    AND a.month_dt = b.month_dt
    AND (a.platform != 'Amazon' OR a.market = b.market)
    LEFT JOIN dw_brand_mapping.final_result c ON c.platform = 'Tmall' 
    AND a.brand_id = CAST(c.brand_id AS STRING)
    AND a.category_1 = CAST(c.category_1 AS STRING)
    LEFT JOIN {{ datahub_dwd_gpt_brand_info_monthly }} d ON a.pfsku_id = d.sku_id
    AND (CASE WHEN lower(a.platform) = 'amazon' THEN lower(a.market) ELSE lower(a.platform) END) = lower(d.platform)
    AND (CASE WHEN lower(a.platform) = 'amazon' THEN a.pfsku_id ELSE a.item_id END) = d.product_id
    AND (lower(a.platform) = 'amazon' OR a.month_dt = d.month_dt)
    WHERE b.item_id IS NOT NULL
    AND {month_condition}

    UNION ALL
    SELECT CAST(xxhash64(concat_ws('||', a.platform, COALESCE(a.market, 'cn'), CAST(a.item_id AS STRING), CAST(a.pfsku_id AS STRING), '1', a.month_dt, CAST(NVL(a.shop_id, '0') AS STRING))) AS BIGINT) AS unique_id,
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
           coalesce(e.std_brand_name, a.brand_name)AS ai_brand_name,
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
           1 AS sku_no,
           CAST(COUNT(1) OVER (PARTITION BY a.month_dt, a.platform, CAST(a.item_id AS STRING), CAST(a.pfsku_id AS STRING), a.shop_id,a.market) AS INT) AS sku_num,
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
           ifnull(d.package_all, 1) as package,
           if(ifnull(d.weight_all_in_g, 0) / ifnull(d.package_all, 1) < {unit_weight_threshold}, ifnull(d.weight_all_in_g, 0) / ifnull(d.package_all, 1), null) as weight,
           if(ifnull(d.weight_all_in_g, 0) / ifnull(d.package_all, 1) < {unit_weight_threshold}, ifnull(d.weight_all_in_g, 0), null) as total_weight,
           if(ifnull(d.weight_all_in_g, 0) / ifnull(d.package_all, 1) < {unit_weight_threshold} OR d.weight_all_in_g IS NULL, IF(d.weight_all_in_g IS NOT NULL, 'GPT克重清洗', '机器清洗'), "超过阈值{unit_weight_threshold}g") as total_weight_src,
           CAST(NULL AS STRING) AS attributes,
           '机器清洗' AS sku_src,
           CAST(NULL AS STRING) AS media,
           CAST(NULL AS STRING) AS std_category_name,
           CAST(NULL AS STRING) AS std_sub_category_name,
           CAST(NULL AS STRING) AS std_brand_name,
           a.manufacturer AS manufacturer,
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
           a.pfsku_title_cn,
           a.sub_category,
           a.sub_category_cn,
           a.category_1_cn,
           a.category_2_cn,
           a.category_3_cn,
           a.category_4_cn,
           a.category_5_cn,
           a.category_6_cn,
           a.platform,
           a.month_dt,
           COALESCE(a.market, 'cn') AS market
    FROM {{ monthly_basic_info }} a LEFT ANTI
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
    LEFT JOIN {{ datahub_dwd_gpt_brand_info_monthly }} e ON a.pfsku_id = e.sku_id
    AND (CASE WHEN lower(a.platform) = 'amazon' THEN lower(a.market) ELSE lower(a.platform) END) = lower(e.platform)
    AND (CASE WHEN lower(a.platform) = 'amazon' THEN a.pfsku_id ELSE a.item_id END) = e.product_id
    AND (lower(a.platform) = 'amazon' OR a.month_dt = e.month_dt)
    WHERE {month_condition}
    """
    df = spark_sql_with_log(spark, query)
    
    # 如果分区条件不完整（少于3个分区字段），查询实际受影响的分区组合
    # 注意：只查询符合当前过滤条件的分区组合，避免刷新不应该刷新的分区
    partition_list = None
    if len(partition_parts) < 3:
        # 构建过滤条件，确保只查询符合当前过滤条件的分区
        filter_conditions = []
        if platform_filter:
            filter_conditions.append(f'platform = "{platform_filter}"')
        if month_dt:
            filter_conditions.append(f'month_dt = "{month_dt}"')
        if market_filter:
            filter_conditions.append(f'market = "{market_filter}"')
        
        # 只查询符合过滤条件的分区组合
        partition_df = df.select('platform', 'month_dt', 'market').distinct()
        if filter_conditions:
            filter_expr = ' AND '.join(filter_conditions)
            partition_df = partition_df.filter(filter_expr)
        
        partition_rows = partition_df.collect()
        partition_list = [(row.platform, row.month_dt, row.market) for row in partition_rows]
    
    return df, month_partition, drop_incremental_stats, partition_list


def loader(spark, df: DataFrame):
    spark_write_hive(df, spark, TARGET_DB, TARGET_TABLE, create_table_ddl=DDL, 
                     dynamic_partition='platform,month_dt,market', emr=EMR,
                     refresh_stats=REFRESH_STATS, compression=COMPRESSION, 
                     repartition_num=PARTITION_NUM)


def main():
    init_logging()
    impala = new_impala_connector(emr=True)
    spark = SparkSession.builder \
        .appName("rearc.{{dag_name}}.{{job_name}}.{{execution_date}}") \
        .enableHiveSupport() \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "512MB") \
        .config("spark.sql.parquet.enableVectorizedReader", "true") \
        .getOrCreate()
    spark.sql(f'set spark.sql.shuffle.partitions={PARTITION_NUM}')
    spark.sql(f'set spark.sql.autoBroadcastJoinThreshold=-1')
    spark.sql(f'set spark.sql.broadcastTimeout=600')

    df, month_partition, drop_incremental_stats, partition_list = dumper(spark, CALC_PARTITION, update_mode, project_start_month)
    loader(spark, df)
    
    spark.stop()
    
    # 刷新分区元数据：如果分区条件完整则直接刷新，否则逐个刷新受影响的分区
    if partition_list is None:
        impala.execute(f'REFRESH {TARGET_DB}.{TARGET_TABLE} PARTITION({month_partition})')
        impala.execute(drop_incremental_stats)
        impala.execute(f'COMPUTE INCREMENTAL STATS {TARGET_DB}.{TARGET_TABLE} PARTITION({month_partition})')
    else:
        for platform, month_dt, market in partition_list:
            partition_clause = f'platform="{platform}",month_dt="{month_dt}",market="{market}"'
            drop_stats_sql = f'DROP INCREMENTAL STATS {TARGET_DB}.{TARGET_TABLE} PARTITION({partition_clause});'
            impala.execute(f'REFRESH {TARGET_DB}.{TARGET_TABLE} PARTITION({partition_clause})')
            impala.execute(drop_stats_sql)
            impala.execute(f'COMPUTE INCREMENTAL STATS {TARGET_DB}.{TARGET_TABLE} PARTITION({partition_clause})')


if __name__ == '__main__':
    main()

