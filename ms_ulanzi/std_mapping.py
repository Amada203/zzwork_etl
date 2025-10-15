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
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
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
    month_condition = f'month_dt = "{month_dt}"'
    month_partition = f'month_dt = "{month_dt}"'
    drop_incremental_stats = f'DROP INCREMENTAL STATS {result_table} PARTITION(month_dt="{month_dt}");'
elif update_mode == 'full':
    month_field = ',month_dt'
    month_condition = '1=1'
    month_partition = 'month_dt'
    drop_incremental_stats = f'DROP INCREMENTAL STATS {result_table} PARTITION(month_dt>="{project_start_month}");'
else:
    raise ValueError(f'update_mode="{update_mode}" is invalid !!!')


# project_id = 281
rule_set_dict = {
    # 'shop_mapping': (104941, read_rule_set(104941),),  
    'media': (104996, read_rule_set(104996),), 
    'std_brand_name': (104998, read_rule_set(104998),),  
    # 'std_category_name': (104943, read_rule_set(104943),), 
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
    
    # 清洗 shop_id、shop_name 字段，应用规则104941
    # value, src = get_mapping_result_and_src(row, 'shop_mapping')
    # if 'new_shop_id' in value and value['new_shop_id']:
    #     row['shop_id'] = value['new_shop_id']
    # if 'new_shop_name' in value and value['new_shop_name']:
    #     row['shop_name'] = value['new_shop_name']
    # row['shop_id_src'] = row['shop_name_src'] = src
    
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
    
    # 清洗 std_category_name 字段，应用规则104943（依赖std_brand_name的结果）
    # value, src = get_mapping_result_and_src(row, 'std_category_name')
    # row['std_category_name'] = value['std_category_name']
    # row['std_category_name_src'] = src
    
           
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


def safe_float(s):
    if s is None:
        return s
    return float(s)


def safe_round(f):
    if f is None:
        return f
    return round(f)


query = f'''
SELECT *
FROM {source_table}
WHERE {month_condition}
'''

SCHEMA = '''
  unique_id                   BIGINT,
  platform                    STRING,
  market                      STRING,
  item_id                     BIGINT,
  pfsku_id                    BIGINT,
  item_title                  STRING,
  pfsku_title                 STRING,
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
  month_dt                    STRING
'''

DDL = f'''
CREATE TABLE IF NOT EXISTS {result_table}(
  unique_id                   BIGINT,
  platform                    STRING,
  market                      STRING,
  item_id                     BIGINT,
  pfsku_id                    BIGINT,
  item_title                  STRING,
  pfsku_title                 STRING,
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

# 根据数据量动态调整分区数，确保综合性能最优
if update_mode == 'incremental':
    # 增量模式：数据量相对较小，使用适中分区数
    initial_partitions = 100
    final_partitions = 8
elif update_mode == 'full':
    # Full模式：20亿数据，每个分区约1亿条记录，目标文件512MB
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
       PACKAGE,
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
       std_spu_name_src
       {month_field}
FROM tv_std_mapping
''')
spark.stop()


impala.execute(f'{{ refresh }} {result_table}')
impala.execute(drop_incremental_stats)
impala.execute(f'compute incremental stats {result_table}')
