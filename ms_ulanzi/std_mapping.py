# -*- coding: utf-8 -*-
import collections
import json
import logging
import unicodedata

from collections import OrderedDict
from pyspark.sql import SparkSession
from pigeon.connector import new_impala_connector
from pigeon.utils import init_logging
from ymrbdt.attributes import read_rule_set

# 配置参数
update_mode = '{{ update_mode }}'
# 查询的分区，支持多种类型 Union[int, str, list, None]
# 表示"不过滤"的值: None, '', '*', 'ALL' (不区分大小写)
CALC_PARTITION = ['{{ month_dt }}', '{{ platform_filter }}', '{{ market_filter }}']
project_start_month = '{{ project_start_month }}'

source_table = '{{ std_mapping_origin }}'
TARGET_DB = '{{ std_mapping }}'.split('.')[0]
TARGET_TABLE = '{{ std_mapping }}'.split('.')[1]
result_table = f'{TARGET_DB}.{TARGET_TABLE}'


def _normalize_filter(value):
    """标准化过滤值，支持 None, '', '*', 'ALL' 等表示不过滤的值"""
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value or value.upper() in ('*', 'ALL', 'NULL', 'NONE'):
            return None
        return value
    return value


def build_partition_conditions(calc_partition, update_mode, project_start_month, result_table):
    """构建分区条件和相关配置
    
    返回:
        month_field: SELECT 中是否需要包含分区字段
        where_condition: WHERE 条件字符串
        partition_clause: INSERT 语句中的分区子句
        drop_stats_sql: 删除统计信息的 SQL
    """
    # 解析分区参数
    if isinstance(calc_partition, list):
        month_dt = _normalize_filter(calc_partition[0] if len(calc_partition) > 0 else None)
        platform_filter = _normalize_filter(calc_partition[1] if len(calc_partition) > 1 else None)
        market_filter = _normalize_filter(calc_partition[2] if len(calc_partition) > 2 else None)
    else:
        month_dt = _normalize_filter(calc_partition)
        platform_filter = None
        market_filter = None
    
    # 构建 WHERE 条件
    where_conditions = []
    
    if update_mode == 'incremental':
        month_field = ''
        if month_dt:
            where_conditions.append(f's.month_dt = "{month_dt}"')
        if platform_filter:
            where_conditions.append(f's.platform = "{platform_filter}"')
        if market_filter:
            where_conditions.append(f's.market = "{market_filter}"')
        where_clause = ' AND '.join(where_conditions) if where_conditions else '1=1'
        
        # 构建分区子句（支持 platform, month_dt, market 三个分区字段）
        partition_parts = []
        if platform_filter:
            partition_parts.append(f'platform = "{platform_filter}"')
        if month_dt:
            partition_parts.append(f'month_dt = "{month_dt}"')
        if market_filter:
            partition_parts.append(f'market = "{market_filter}"')
        partition_clause = ','.join(partition_parts) if partition_parts else 'platform,month_dt,market'
        
        # 构建 DROP INCREMENTAL STATS 的 PARTITION 子句
        if partition_parts:
            drop_partition_clause = ','.join(partition_parts)
            drop_stats_sql = f'DROP INCREMENTAL STATS {result_table} PARTITION({drop_partition_clause});'
        else:
            drop_stats_sql = ''
            
    elif update_mode == 'full':
        month_field = ',s.platform,s.month_dt,s.market'
        if month_dt:
            where_conditions.append(f's.month_dt >= "{project_start_month}"')
        else:
            where_conditions.append('s.month_dt IS NOT NULL')
        if platform_filter:
            where_conditions.append(f's.platform = "{platform_filter}"')
        if market_filter:
            where_conditions.append(f's.market = "{market_filter}"')
        where_clause = ' AND '.join(where_conditions) if where_conditions else '1=1'
        
        # 全量模式使用动态分区
        partition_clause = 'platform,month_dt,market'
        
        # 构建 DROP INCREMENTAL STATS 的 PARTITION 子句（与 WHERE 条件保持一致）
        drop_partition_parts = []
        if platform_filter:
            drop_partition_parts.append(f'platform="{platform_filter}"')
        drop_partition_parts.append(f'month_dt>="{project_start_month}"')
        if market_filter:
            drop_partition_parts.append(f'market="{market_filter}"')
        drop_partition_clause = ','.join(drop_partition_parts)
        drop_stats_sql = f'DROP INCREMENTAL STATS {result_table} PARTITION({drop_partition_clause});'
    else:
        raise ValueError(f'update_mode="{update_mode}" is invalid !!!')
    
    return month_field, where_clause, partition_clause, drop_stats_sql


DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_DB}.{TARGET_TABLE} (
  unique_id                   BIGINT,
  platform                    STRING,
  market                      STRING,
  item_id                     STRING,
  pfsku_id                    STRING,
  item_title                  STRING,
  pfsku_title                 STRING,
  pfsku_title_cn              STRING,
  category_1                  STRING,
  category_2                  STRING,
  category_3                  STRING,
  category_4                  STRING,
  category_5                  STRING,
  category_6                  STRING,
  category_1_cn               STRING,
  category_2_cn               STRING,
  category_3_cn               STRING,
  category_4_cn               STRING,
  category_5_cn               STRING,
  category_6_cn               STRING,
  sub_category                STRING,
  sub_category_cn             STRING,
  category_1_id               STRING,
  category_2_id               STRING,
  category_3_id               STRING,
  category_4_id               STRING,
  category_5_id               STRING,
  category_6_id               STRING,
  category_id                 BIGINT,
  category_name               STRING,
  shop_id                     STRING,
  shop_name                   STRING,
  unique_shop_name            STRING,
  shop_type                   STRING,
  brand_id                    STRING,
  brand_name                  STRING,
  ai_brand_name               STRING,
  properties                  STRING,
  shop_url                    STRING,
  item_url                    STRING,
  pfsku_url                   STRING,
  item_image                  STRING,
  item_images                 STRING,
  pfsku_image                 STRING,
  tags                        STRING,
  basic_info                  STRING,
  recommend_remark            STRING,
  sku_no                      INT,
  sku_num                     INT,
  sku_image                   STRING,
  sku_title                   STRING,
  sku_value_ratio             DOUBLE,
  sku_value_ratio_src         STRING,
  is_bundle                   INT,
  is_gift                     INT,
  package                     INT,
  weight                      DOUBLE,
  total_weight                DOUBLE,
  total_weight_src            STRING,
  attributes                  STRING,
  sku_src                     STRING,
  media                       STRING,
  std_category_name           STRING,
  std_sub_category_name       STRING,
  std_brand_name              STRING,
  manufacturer                STRING,
  variant                     STRING,
  std_spu_name                STRING,
  std_sku_name                STRING,
  media_src                   STRING,
  std_category_name_src       STRING,
  std_sub_category_name_src   STRING,
  std_brand_name_src          STRING,
  manufacturer_src            STRING,
  variant_src                 STRING,
  std_spu_name_src            STRING,
  std_category_1              STRING,
  std_category_2              STRING,
  std_category_3              STRING,
  std_category_4              STRING,
  std_category_5              STRING,
  std_category_6              STRING
) PARTITIONED BY (platform STRING, month_dt STRING, market STRING) 
COMMENT 'OneMap标准字段表' 
STORED AS PARQUET
"""


def deal_with_brand_name(brand_name):
    if brand_name is None:
        return brand_name
    return unicodedata.normalize('NFKC', brand_name)


def get_mapping_result_and_src(row, rule_set_key, rule_dict):
    rule_set_id = rule_dict[rule_set_key][0]
    rule_set = rule_dict[rule_set_key][1]

    rv = rule_set.transform(row)[0]
    value_dct = {}
    for col in rule_set.result_column:
        value_dct[col] = rv[col]

    if rv['__matched_rule'] == '__DEFAULT__':
        priority = '__DEFAULT__'
    else:
        if isinstance(rv['__matched_rule'], list):
            priority = ','.join([str(json.loads(x).get('priority')) for x in rv['__matched_rule']])
        else:
            priority = json.loads(rv['__matched_rule'])['priority']

    src = f'{rule_set_id},{priority}'
    return value_dct, src


def transform_item(row, item_rule_set_dict):
    # 清洗 media 字段，应用规则104996
    value, src = get_mapping_result_and_src(row, 'media', item_rule_set_dict)
    if 'media' in value and value['media']:
        row['media'] = value['media']
        row['media_src'] = src
    return row


def transform_pfsku(row, pfsku_rule_set_dict):
    if row.get('market') == 'JP':
        if 'brand_name' in row and row['brand_name'] is not None:
            row['brand_name'] = deal_with_brand_name(row['brand_name'])
        if 'ai_brand_name' in row and row['ai_brand_name'] is not None:
            row['ai_brand_name'] = deal_with_brand_name(row['ai_brand_name'])
    
    # 清洗 std_brand_name 字段，应用规则104998（使用已处理的 brand_name）
    value, src = get_mapping_result_and_src(row, 'std_brand_name', pfsku_rule_set_dict)
    if 'std_brand_name' in value and value['std_brand_name']:
        row['std_brand_name'] = value['std_brand_name']
        row['std_brand_name_src'] = src

    # 默认填充新增标准品类字段
    for col in ['std_category_1', 'std_category_2', 'std_category_3', 'std_category_4', 'std_category_5', 'std_category_6']:
        row.setdefault(col, None)
    row.setdefault('std_category_name', row.get('std_category_name'))

    # 清洗标准品类字段，应用规则105117
    value, _ = get_mapping_result_and_src(row, 'std_category', pfsku_rule_set_dict)
    for col in ['std_category_1', 'std_category_2', 'std_category_3', 'std_category_4', 'std_category_5', 'std_category_6', 'std_category_name']:
        if col in value and value[col]:
            row[col] = value[col]

    # 清洗中文品类字段，应用规则105116
    value, _ = get_mapping_result_and_src(row, 'category_cn', pfsku_rule_set_dict)
    for col in ['category_1_cn', 'category_2_cn', 'category_3_cn', 'category_4_cn', 'category_5_cn', 'category_6_cn', 'sub_category_cn']:
        if col in value and value[col]:
            row[col] = value[col]
    return row


def _spark_row_to_ordereddict(row):
    return collections.OrderedDict(zip(row.__fields__, row))


def sql_with_log(sql, executor, params=None):
    logging.info(sql)
    if not params:
        return executor(sql)
    return executor(sql, params)


def refresh_partition_stats(impala, result_table, partition_clause, drop_stats_sql=None):
    """刷新单个分区的 Impala 元数据和统计信息"""
    try:
        impala.execute(f'{{ refresh }} {result_table} PARTITION({partition_clause})')
        if drop_stats_sql:
            impala.execute(drop_stats_sql)
        impala.execute(f'compute incremental stats {result_table} PARTITION({partition_clause})')
    except Exception as e:
        logging.warning(f'Failed to refresh partition {partition_clause}: {e}')


def dumper(spark, calc_partition, update_mode, project_start_month, source_table):
    """查询和处理数据"""
    # 构建分区条件
    month_field, where_condition, partition_clause, drop_stats_sql = build_partition_conditions(
        calc_partition, update_mode, project_start_month, result_table
    )
    
    # 初始化规则字典
    item_rule_set_dict = {
        'media': (104996, read_rule_set(104996)),
    }
    
    pfsku_rule_set_dict = {
        'std_category': (105117, read_rule_set(105117)),
        'category_cn': (105116, read_rule_set(105116)),
        'std_brand_name': (104998, read_rule_set(104998)),
    }
    
    # 定义 Schema
    ITEM_SCHEMA = '''
      platform                    STRING,
      market                      STRING,
      shop_id                     STRING,
      shop_name                   STRING,
      item_title                  STRING,
      brand_name                  STRING,
      brand_id                    STRING,
      category_id                 BIGINT,
      item_id                     STRING,
      category_1                  STRING,
      category_2                  STRING,
      category_3                  STRING,
      category_4                  STRING,
      category_1_id               STRING,
      category_2_id               STRING,
      category_3_id               STRING,
      category_4_id               STRING,
      ai_brand_name               STRING,
      media                       STRING,
      std_brand_name              STRING,
      media_src                   STRING,
      std_brand_name_src          STRING,
      month_dt                    STRING
    '''
    
    PFSKU_SCHEMA = '''
      platform                    STRING,
      market                      STRING,
      manufacturer                STRING,
      shop_id                     STRING,
      shop_name                   STRING,
      item_title                  STRING,
      item_id                     STRING,
      pfsku_id                    STRING,
      brand_name                  STRING,
      brand_id                    STRING,
      category_id                 bigint,
      category_1                  STRING,
      category_2                  STRING,
      category_3                  STRING,
      category_4                  STRING,
      category_5                  STRING,
      category_6                  STRING,
      category_1_id               STRING,
      category_2_id               STRING,
      category_3_id               STRING,
      category_4_id               STRING,
      ai_brand_name               STRING,
      std_category_1              STRING,
      std_category_2              STRING,
      std_category_3              STRING,
      std_category_4              STRING,
      std_category_5              STRING,
      std_category_6              STRING,
      category_name               STRING,
      sub_category                STRING,
      category_1_cn               STRING,
      category_2_cn               STRING,
      category_3_cn               STRING,
      category_4_cn               STRING,
      category_5_cn               STRING,
      category_6_cn               STRING,
      std_category_name           STRING,
      sub_category_cn             STRING,
      std_brand_name              STRING,
      std_brand_name_src          STRING,
      month_dt                    STRING
    '''
    
    # 构建查询
    item_query = f'''
    SELECT platform,
           market,
           shop_id,
           shop_name,
           item_title,
           brand_name,
           brand_id,
           category_id,
           item_id,
           category_1,
           category_2,
           category_3,
           category_4,
           category_1_id,
           category_2_id,
           category_3_id,
           category_4_id,
           ai_brand_name,
           media,
           std_brand_name,
           media_src,
           std_brand_name_src,
           month_dt
    FROM (
        SELECT platform,
               market,
               shop_id,
               shop_name,
               item_title,
               brand_name,
               brand_id,
               category_id,
               item_id,
               category_1,
               category_2,
               category_3,
               category_4,
               category_1_id,
               category_2_id,
               category_3_id,
               category_4_id,
               ai_brand_name,
               media,
               std_brand_name,
               media_src,
               std_brand_name_src,
               month_dt,
               unique_id,
               ROW_NUMBER() OVER (PARTITION BY platform,market, item_id, IF(lower(platform) = 'suning', shop_id, '0'), month_dt ORDER BY unique_id ) as rn
        FROM {source_table} s
        WHERE {where_condition}
    ) t
    WHERE rn = 1
    '''
    
    pfsku_query = f'''
    SELECT platform,
           market,
           manufacturer,
           shop_id,
           shop_name,
           item_title,
           item_id,
           pfsku_id,
           brand_name,
           brand_id,
           category_id,
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
           ai_brand_name,
           null as std_category_1,
           null as std_category_2,
           null as std_category_3,
           null as std_category_4,
           null as std_category_5,
           null as std_category_6,
           category_name,
           sub_category,
           category_1_cn,
           category_2_cn,
           category_3_cn,
           category_4_cn,
           category_5_cn,
           category_6_cn,
           std_category_name,
           sub_category_cn,
           std_brand_name,
           month_dt
    FROM {source_table} s
    WHERE {where_condition}
    '''
    
    # 确定分区数
    if update_mode == 'incremental':
        initial_partitions = 100
        final_partitions = 8
    elif update_mode == 'full':
        initial_partitions = 400
        final_partitions = 15
    
    # 处理 item 数据
    def apply_item_transformation(rows):
        for row in rows:
            yield transform_item(_spark_row_to_ordereddict(row), item_rule_set_dict)
    
    item_rdd = spark.sql(item_query).repartition(initial_partitions).rdd.mapPartitions(apply_item_transformation)
    item_df = spark.createDataFrame(item_rdd, ITEM_SCHEMA)
    item_df.repartition(final_partitions).createOrReplaceTempView('tv_std_mapping_item')
    
    # 处理 pfsku 数据
    def apply_pfsku_transformation(rows):
        for row in rows:
            yield transform_pfsku(_spark_row_to_ordereddict(row), pfsku_rule_set_dict)
    
    pfsku_rdd = spark.sql(pfsku_query).repartition(initial_partitions).rdd.mapPartitions(apply_pfsku_transformation)
    pfsku_df = spark.createDataFrame(pfsku_rdd, PFSKU_SCHEMA)
    pfsku_df.repartition(final_partitions).createOrReplaceTempView('tv_std_mapping_pfsku')
    
    # 如果是动态分区模式，查询可能的分区组合（用于后续 Impala 刷新）
    partition_list = None
    if partition_clause == 'platform,month_dt,market':
        # 查询 source_table 中符合条件的分区组合
        partition_query = f'''
        SELECT DISTINCT platform, month_dt, market
        FROM {source_table}
        WHERE {where_condition}
        '''
        partition_rows = spark.sql(partition_query).collect()
        partition_list = [{'platform': row.platform, 'month_dt': row.month_dt, 'market': row.market} for row in partition_rows]
    
    return month_field, where_condition, partition_clause, drop_stats_sql, partition_list


def loader(spark, month_field, where_condition, partition_clause, drop_stats_sql, partition_list, result_table, source_table):
    """写入数据和刷新 Impala"""
    spark.sql(DDL)
    spark.sql('SET spark.sql.parquet.compression.codec=gzip')
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    
    insert_sql = f'''
    INSERT OVERWRITE TABLE {result_table} PARTITION({partition_clause})
    SELECT s.unique_id,
           s.platform,
           s.market,
           s.item_id,
           s.pfsku_id,
           s.item_title,
           s.pfsku_title,
           s.pfsku_title_cn,
           TRIM(REGEXP_REPLACE(COALESCE(s.category_1, ''), '\r|\n', '')) AS category_1,
           TRIM(REGEXP_REPLACE(COALESCE(s.category_2, ''), '\r|\n', '')) AS category_2,
           TRIM(REGEXP_REPLACE(COALESCE(s.category_3, ''), '\r|\n', '')) AS category_3,
           TRIM(REGEXP_REPLACE(COALESCE(s.category_4, ''), '\r|\n', '')) AS category_4,
           TRIM(REGEXP_REPLACE(COALESCE(s.category_5, ''), '\r|\n', '')) AS category_5,
           TRIM(REGEXP_REPLACE(COALESCE(s.category_6, ''), '\r|\n', '')) AS category_6,
           TRIM(REGEXP_REPLACE(COALESCE(p.category_1_cn, s.category_1_cn, ''), '\r|\n', '')) AS category_1_cn,
           TRIM(REGEXP_REPLACE(COALESCE(p.category_2_cn, s.category_2_cn, ''), '\r|\n', '')) AS category_2_cn,
           TRIM(REGEXP_REPLACE(COALESCE(p.category_3_cn, s.category_3_cn, ''), '\r|\n', '')) AS category_3_cn,
           TRIM(REGEXP_REPLACE(COALESCE(p.category_4_cn, s.category_4_cn, ''), '\r|\n', '')) AS category_4_cn,
           TRIM(REGEXP_REPLACE(COALESCE(p.category_5_cn, s.category_5_cn, ''), '\r|\n', '')) AS category_5_cn,
           TRIM(REGEXP_REPLACE(COALESCE(p.category_6_cn, s.category_6_cn, ''), '\r|\n', '')) AS category_6_cn,
           TRIM(REGEXP_REPLACE(COALESCE(s.sub_category, ''), '\r|\n', '')) AS sub_category,
           TRIM(REGEXP_REPLACE(COALESCE(p.sub_category_cn, s.sub_category_cn, ''), '\r|\n', '')) AS sub_category_cn,
           s.category_1_id,
           s.category_2_id,
           s.category_3_id,
           s.category_4_id,
           s.category_5_id,
           s.category_6_id,
           s.category_id,
           s.category_name,
           s.shop_id,
           s.shop_name,
           s.unique_shop_name,
           s.shop_type,
           s.brand_id,
           p.brand_name as brand_name,
           COALESCE(p.ai_brand_name, s.ai_brand_name) as ai_brand_name,
           s.properties,
           s.shop_url,
           s.item_url,
           s.pfsku_url,
           s.item_image,
           s.item_images,
           s.pfsku_image,
           s.tags,
           s.basic_info,
           s.recommend_remark,
           s.sku_no,
           s.sku_num,
           s.sku_image,
           s.sku_title,
           s.sku_value_ratio,
           s.sku_value_ratio_src,
           s.is_bundle,
           s.is_gift,
           s.PACKAGE,
           s.weight,
           s.total_weight,
           s.total_weight_src,
           s.attributes,
           s.sku_src,
           COALESCE(t.media, s.media) as media,
           COALESCE(p.std_category_name, s.std_category_name) AS std_category_name,
           s.std_sub_category_name,
           COALESCE(u.std_brand_name, p.std_brand_name, s.std_brand_name) as std_brand_name,
           s.manufacturer,
           s.variant,
           s.std_spu_name,
           s.std_sku_name,
           COALESCE(t.media_src, s.media_src) as media_src,
           s.std_category_name_src,
           s.std_sub_category_name_src,
           CASE
               WHEN u.std_brand_name IS NOT NULL THEN 'ulanzi_std_brand_mapping'
               WHEN p.std_brand_name_src IS NOT NULL THEN p.std_brand_name_src
               ELSE s.std_brand_name_src
           END as std_brand_name_src,
           s.manufacturer_src,
           s.variant_src,
           s.std_spu_name_src,
           p.std_category_1,
           p.std_category_2,
           p.std_category_3,
           p.std_category_4,
           p.std_category_5,
           p.std_category_6
           {month_field}
    FROM {source_table} s
    LEFT JOIN tv_std_mapping_pfsku p ON s.platform = p.platform
    AND s.market <=> p.market
    AND s.item_id = p.item_id
    AND s.pfsku_id = p.pfsku_id
    AND s.month_dt = p.month_dt
    LEFT JOIN (select distinct * from {{ulanzi_std_brand_mapping}}) u ON p.brand_name = u.brand_name
    LEFT JOIN tv_std_mapping_item t ON s.platform = t.platform 
    AND s.item_id = t.item_id 
    AND IF(lower(s.platform) = 'suning', s.shop_id, '0') = IF(lower(t.platform) = 'suning', t.shop_id, '0')
    AND s.month_dt = t.month_dt
    AND s.market <=> t.market
    WHERE {where_condition}
    '''
    
    sql_with_log(insert_sql, spark.sql)
    
    # Impala 刷新和统计信息更新
    spark.stop()
    impala = new_impala_connector()
    
    # 注意：REFRESH 和 COMPUTE INCREMENTAL STATS 需要明确的分区值，不能使用动态分区
    if partition_list is not None:
        # 动态分区模式：使用预先查询的分区列表，逐个刷新
        for partition in partition_list:
            partition_clause_single = f'platform="{partition["platform"]}",month_dt="{partition["month_dt"]}",market="{partition["market"]}"'
            drop_stats_sql_single = f'DROP INCREMENTAL STATS {result_table} PARTITION({partition_clause_single});'
            refresh_partition_stats(impala, result_table, partition_clause_single, drop_stats_sql_single)
    else:
        # 静态分区模式：直接刷新指定的分区
        refresh_partition_stats(impala, result_table, partition_clause, drop_stats_sql)


def main():
    init_logging()
    spark = SparkSession.builder \
        .appName("onework.{{dag_name}}.{{job_name}}.{{ execution_date }}") \
        .enableHiveSupport() \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "512MB") \
        .config("spark.sql.parquet.enableVectorizedReader", "true") \
        .getOrCreate()
    
    # 查询和处理数据
    month_field, where_condition, partition_clause, drop_stats_sql, partition_list = dumper(
        spark, CALC_PARTITION, update_mode, project_start_month, source_table
    )
    
    # 写入数据
    loader(spark, month_field, where_condition, partition_clause, drop_stats_sql, partition_list, result_table, source_table)


if __name__ == '__main__':
    exit(main())
