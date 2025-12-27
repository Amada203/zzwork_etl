# -*- coding: utf-8 -*-

import collections
import copy
import json

from collections import OrderedDict
from pyspark.sql import SparkSession
from pigeon.connector import new_impala_connector
from ymrbdt.attributes import read_rule_set


spark = SparkSession.builder.appName("onework.{{dag_name}}.{{job_name}}.{{ execution_date }}").enableHiveSupport().getOrCreate()
impala = new_impala_connector()

update_mode = '{{ update_mode }}'
month_dt = '{{ month_dt }}'
project_start_month = '{{ project_start_month }}'

source_table = '{{ods_aigc_result_data}}'
result_table = '{{scj_pfsku_package_splited}}'


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


# 规则集配置 - 按照执行顺序配置
rule_set_dict = {
    'std_category': (104985, read_rule_set(104985),),  
    'form': (104986, read_rule_set(104986),),
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
    

    # 标准品类清洗 - 处理std_category_1, std_category_2, std_category_3, std_category_4
    value, src = get_mapping_result_and_src(row, 'std_category')
    for i in range(1, 5):  # std_category_1, std_category_2, std_category_3, std_category_4
        if f'std_category_{i}' in value and value[f'std_category_{i}'] not in [None, 'NULL', '']:
            row[f'std_category_{i}'] = value[f'std_category_{i}']
            row['std_category_src'] = src
    
    
    # 产品属性清洗 - 依赖规则集104986，只保留有值的字段
    value, src = get_mapping_result_and_src(row, 'form')
    attribute_fields = ['form', 'target_format']
    
    # 只保留有值的字段进行赋值，规则集104986结果会覆盖上游字段值
    for field in attribute_fields:
        if field in value and value[field] not in [None, 'NULL', '']:
            row[field] = value[field]
            row['attribute_src'] = src
    
    
    
    return row


def _spark_row_to_ordereddict(row):
    """将Spark Row转换为OrderedDict"""
    return collections.OrderedDict(zip(row.__fields__, row))


def apply_transformation(rows):
    """应用数据转换"""
    for row in rows:
        yield transform(_spark_row_to_ordereddict(row))


def safe_int(s):
    """安全的整数转换"""
    if s is None:
        return s
    return int(s)


def safe_float(s):
    """安全的浮点数转换"""
    if s is None:
        return s
    return float(s)


def safe_round(f):
    """安全的四舍五入"""
    if f is None:
        return f
    return round(f)


# 主查询SQL - 根据字段需求进行调整，剔除hash_id字段，但保留规则集需要的字段
query = f'''
SELECT s.platform,
       s.item_id,
       s.pfsku_id,
       s.item_title,
       s.pfsku_title,
       s.first_image,
       s.pfsku_image,
       s.category_name,
       s.shop_name,
       s.brand_name,
       s.item_url,
       s.pfsku_url,
       s.tags,
       s.top80,
       s.month_start,
       s.std_category_1,
       s.std_category_2,
       s.std_category_3,
       s.std_category_4,
       null as std_category_src,
       s.std_brand_name,
       s.std_spu_name,
       s.flavor,
       s.std_group_spu_name,
       s.is_bundle,
       s.is_gift,
       s.sku_no,
       s.bundle_name,
       s.weight,
       s.weight_unit,
       s.package_num,
       s.package_unit,
       s.unit_weight,
       s.quantity,
       s.quantity_unit,
       s.form,
       s.need_toilet_brush,
       s.need_with_water,
       s.use_method,
       s.spray_form,
       s.texture,
       s.benefit,
       s.target_user,
       s.product_scenarios,
       s.disinfectant_claim,
       null as target_format,
       null as attribute_src,
       s.pfsku_value_sales,
       s.pfsku_unit_sales,
       s.pfsku_discount_price,
       s.month_dt
FROM {source_table} s
WHERE {month_condition}
'''

# Schema定义 - 根据字段需求进行调整，剔除hash_id字段，但保留规则集需要的字段
SCHEMA = '''
  platform                     STRING,
  item_id                      BIGINT,
  pfsku_id                     BIGINT,
  item_title                   STRING,
  pfsku_title                  STRING,
  first_image                  STRING,
  pfsku_image                  STRING,
  category_name                STRING,
  shop_name                    STRING,
  brand_name                   STRING,
  item_url                     STRING,
  pfsku_url                    STRING,
  tags                         STRING,
  top80                        DOUBLE,
  month_start                  STRING,
  std_category_1               STRING,
  std_category_2               STRING,
  std_category_3               STRING,
  std_category_4               STRING,
  std_category_src              STRING,
  std_brand_name               STRING,
  std_spu_name                 STRING,
  flavor                       STRING,
  std_group_spu_name           STRING,
  is_bundle                    INT,
  is_gift                      INT,
  sku_no                       INT,
  bundle_name                  STRING,
  weight                       DOUBLE,
  weight_unit                  STRING,
  package_num                  INT,
  package_unit                 STRING,
  unit_weight                  DOUBLE,
  quantity                     INT,
  quantity_unit                STRING,
  form                         STRING,
  need_toilet_brush            STRING,
  need_with_water              STRING,
  use_method                   STRING,
  spray_form                   STRING,
  texture                      STRING,
  benefit                      STRING,
  target_user                  STRING,
  product_scenarios             STRING,
  disinfectant_claim           STRING,
  target_format                STRING,
  attribute_src                STRING,
  pfsku_value_sales            DOUBLE,
  pfsku_unit_sales             INT,
  pfsku_discount_price        DOUBLE,
  month_dt                     STRING
'''

# DDL定义 - 严格按照字段需求，剔除hash_id字段
DDL = f'''
CREATE TABLE IF NOT EXISTS {result_table} (
    platform STRING,
    item_id BIGINT,
    pfsku_id BIGINT,
    item_title STRING,
    pfsku_title STRING,
    first_image STRING,
    pfsku_image STRING,
    category_name STRING,
    shop_name STRING,
    brand_name STRING,
    item_url STRING,
    pfsku_url STRING,
    tags STRING,
    top80 DOUBLE,
    month_start STRING,
    std_category_1 STRING,
    std_category_2 STRING,
    std_category_3 STRING,
    std_category_src STRING,
    std_brand_name STRING,
    std_spu_name STRING,
    flavor STRING,
    std_group_spu_name STRING,
    is_bundle INT,
    is_gift INT,
    sku_no INT,
    bundle_name STRING,
    weight DOUBLE,
    weight_unit STRING,
    package_num INT,
    package_unit STRING,
    unit_weight DOUBLE,
    quantity INT,
    quantity_unit STRING,
    form STRING,
    target_format STRING,
    attribute_src STRING,
    pfsku_value_sales DOUBLE,
    pfsku_unit_sales INT,
    pfsku_discount_price DOUBLE
) PARTITIONED BY (month_dt STRING) 
COMMENT 'SCJ子产品包拆分表' 
STORED AS PARQUET
'''

# 执行数据转换
rdd = spark.sql(query).repartition(200).rdd.mapPartitions(apply_transformation)
df = spark.createDataFrame(rdd, SCHEMA)
df.repartition(4).createOrReplaceTempView('tv_scj_pfsku_package_splited')

# 创建目标表
spark.sql(DDL)
spark.sql('SET spark.sql.parquet.compression.codec=snappy')
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

# 插入数据到目标表 - 只选择最终需要的字段
spark.sql(f'''
INSERT OVERWRITE TABLE {result_table} PARTITION({month_partition})
SELECT platform,
       item_id,
       pfsku_id,
       item_title,
       pfsku_title,
       first_image,
       pfsku_image,
       category_name,
       shop_name,
       brand_name,
       item_url,
       pfsku_url,
       tags,
       top80,
       month_start,
       std_category_1,
       std_category_2,
       std_category_3,
       std_category_src,
       std_brand_name,
       std_spu_name,
       flavor,
       std_group_spu_name,
       is_bundle,
       is_gift,
       sku_no,
       bundle_name,
       weight,
       weight_unit,
       package_num,
       package_unit,
       unit_weight,
       quantity,
       quantity_unit,
       form,
       target_format,
       attribute_src,
       pfsku_value_sales,
       pfsku_unit_sales,
       pfsku_discount_price
       {month_field}
FROM tv_scj_pfsku_package_splited
''')

spark.stop()

# Impala统计更新
impala.execute(f'invalidate metadata {result_table}')
impala.execute(drop_incremental_stats)
impala.execute(f'compute incremental stats {result_table}')
