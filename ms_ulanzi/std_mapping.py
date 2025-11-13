import collections
import copy
import json

from collections import OrderedDict
from pyspark.sql import SparkSession
from pigeon.connector import new_impala_connector
from ymrbdt.attributes import read_rule_set

# 性能优化配置
spark = SparkSession.builder \
    .appName("onework.{{dag_name}}.{{job_name}}.{{ execution_date }}") \
    .enableHiveSupport() \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "512MB") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()
impala = new_impala_connector()

update_mode = '{{ update_mode }}'
month_dt = '{{ month_dt }}'
project_start_month = '{{ project_start_month }}'

source_table = '{{ std_mapping_origin }}'
result_table = '{{ std_mapping }}'

# 增量更新 或者 全量更新
if update_mode == 'incremental':
    month_field = ''
    month_condition = f's.month_dt = "{month_dt}"'
    month_partition = f'month_dt = "{month_dt}"'
    drop_incremental_stats = f'DROP INCREMENTAL STATS {result_table} PARTITION(month_dt="{month_dt}");'
elif update_mode == 'full':
    month_field = ',s.month_dt'
    month_condition = 's.month_dt IS NOT NULL'
    month_partition = 'month_dt'
    drop_incremental_stats = f'DROP INCREMENTAL STATS {result_table} PARTITION(month_dt>="{project_start_month}");'
else:
    raise ValueError(f'update_mode="{update_mode}" is invalid !!!')


rule_set_dict = {
    'media': (104996, read_rule_set(104996),), 
    'std_brand_name': (104998, read_rule_set(104998),),  
}


def get_mapping_result_and_src(row, rule_set_key):
    rule_set_id = rule_set_dict[rule_set_key][0]
    rule_set = rule_set_dict[rule_set_key][1]

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


def transform(row):
    # 清洗 media 字段，应用规则104996
    value, src = get_mapping_result_and_src(row, 'media')
    if 'media' in value and value['media']:
        row['media'] = value['media']
        row['media_src'] = src
    
    # 清洗 std_brand_name 字段，应用规则104998
    value, src = get_mapping_result_and_src(row, 'std_brand_name')
    if 'std_brand_name' in value and value['std_brand_name']:
        row['std_brand_name'] = value['std_brand_name']
        row['std_brand_name_src'] = src
    
    return row


def _spark_row_to_ordereddict(row):
    return collections.OrderedDict(zip(row.__fields__, row))


def apply_transformation(rows):
    for row in rows:
        yield transform(_spark_row_to_ordereddict(row))


def safe_int(s):
    if s is None:
        return s
    return int(s)  


query = f'''
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
           media,
           std_brand_name,
           media_src,
           std_brand_name_src,
           month_dt,
           unique_id,
           ROW_NUMBER() OVER (PARTITION BY platform,market, item_id, IF(lower(platform) = 'suning', shop_id, '0'), month_dt ORDER BY unique_id ) as rn
    FROM {source_table} s
    WHERE {month_condition}
) t
WHERE rn = 1
'''

SCHEMA = '''
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
  media                       STRING,
  std_brand_name              STRING,
  media_src                   STRING,
  std_brand_name_src          STRING,
  month_dt                    STRING
'''

DDL = f'''
CREATE TABLE IF NOT EXISTS {result_table}(
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
  std_spu_name_src            STRING
) PARTITIONED BY (month_dt STRING) 
COMMENT 'OneMap标准字段表' 
STORED AS PARQUET
'''

if update_mode == 'incremental':
    initial_partitions = 100
    final_partitions = 8
elif update_mode == 'full':
    initial_partitions = 400
    final_partitions = 15

rdd = spark.sql(query).repartition(initial_partitions).rdd.mapPartitions(apply_transformation)
df = spark.createDataFrame(rdd, SCHEMA)
df.repartition(final_partitions).createOrReplaceTempView('tv_std_mapping')

spark.sql(DDL)
spark.sql('SET spark.sql.parquet.compression.codec=gzip')
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql(f'''
INSERT OVERWRITE TABLE {result_table} PARTITION({month_partition})
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
       TRIM(REGEXP_REPLACE(COALESCE(s.category_1_cn, ''), '\r|\n', '')) AS category_1_cn,
       TRIM(REGEXP_REPLACE(COALESCE(s.category_2_cn, ''), '\r|\n', '')) AS category_2_cn,
       TRIM(REGEXP_REPLACE(COALESCE(s.category_3_cn, ''), '\r|\n', '')) AS category_3_cn,
       TRIM(REGEXP_REPLACE(COALESCE(s.category_4_cn, ''), '\r|\n', '')) AS category_4_cn,
       TRIM(REGEXP_REPLACE(COALESCE(s.category_5_cn, ''), '\r|\n', '')) AS category_5_cn,
       TRIM(REGEXP_REPLACE(COALESCE(s.category_6_cn, ''), '\r|\n', '')) AS category_6_cn,
       TRIM(REGEXP_REPLACE(COALESCE(s.sub_category, ''), '\r|\n', '')) AS sub_category,
       TRIM(REGEXP_REPLACE(COALESCE(s.sub_category_cn, ''), '\r|\n', '')) AS sub_category_cn,
       s.category_1_id,
       s.category_2_id,
       s.category_3_id,
       s.category_4_id,
       s.category_5_id,
       s.category_6_id,
       s.category_id,
       TRIM(REGEXP_REPLACE(COALESCE(s.category_name, ''), '\r|\n', '')) AS category_name,
       s.shop_id,
       s.shop_name,
       s.unique_shop_name,
       s.shop_type,
       s.brand_id,
       s.brand_name,
       s.ai_brand_name,
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
       s.std_category_name,
       s.std_sub_category_name,
       COALESCE(t.std_brand_name, s.std_brand_name) as std_brand_name,
       s.manufacturer,
       s.variant,
       s.std_spu_name,
       s.std_sku_name,
       COALESCE(t.media_src, s.media_src) as media_src,
       s.std_category_name_src,
       s.std_sub_category_name_src,
       COALESCE(t.std_brand_name_src, s.std_brand_name_src) as std_brand_name_src,
       s.manufacturer_src,
       s.variant_src,
       s.std_spu_name_src
       {month_field}
FROM {source_table} s
LEFT JOIN tv_std_mapping t ON s.platform = t.platform 
AND s.item_id = t.item_id 
AND IF(lower(s.platform) = 'suning', s.shop_id, '0') = IF(lower(t.platform) = 'suning', t.shop_id, '0')
AND s.month_dt = t.month_dt
AND s.market <=> t.market
WHERE {month_condition}
''')
spark.stop()


impala.execute(f'{{ refresh }} {result_table} PARTITION({month_partition})')
impala.execute(drop_incremental_stats)
impala.execute(f'compute incremental stats {result_table} PARTITION({month_partition})')
