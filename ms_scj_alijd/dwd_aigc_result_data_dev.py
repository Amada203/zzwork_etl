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


project_start_month = '{{ project_start_month }}'

source_table = '    '
result_table = 'ms_scj_alijd.dwd_aigc_result_data'

update_mode = 'full'
# 增量更新 或者 全量更新
if update_mode == 'incremental':
    month_field = ''
    month_condition = f's.month_dt = "{month_dt}"'
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
    'std_category_priority1': (104997, read_rule_set(104997),),  # 品类规则优先级1
    'std_category_priority2': (104981, read_rule_set(104981),),  # 品类规则优先级2
    'std_brand_name': (104982, read_rule_set(104982),),  
    'std_spu_name': (104983, read_rule_set(104983),), 
    'product_attributes': (104984, read_rule_set(104984),),
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
    

    # 标准品类清洗 - 双规则优先级
    # 1. 品类规则优先级1 (规则集104997)
    value1, src1 = get_mapping_result_and_src(row, 'std_category_priority1')
    category_src_parts = []
    
    # 处理优先级1的结果字段
    for i in range(1, 3):  # std_category_1, std_category_2
        if f'std_category_{i}' in value1 and value1[f'std_category_{i}'] not in [None, 'NULL', '']:
            row[f'std_category_{i}'] = value1[f'std_category_{i}']
    
    if any(f'std_category_{i}' in value1 and value1[f'std_category_{i}'] not in [None, 'NULL', ''] for i in range(1, 3)):
        category_src_parts.append(src1)
    
    # 2. 品类规则优先级2 (规则集104981)
    value2, src2 = get_mapping_result_and_src(row, 'std_category_priority2')
    
    # 处理优先级2的结果字段
    for i in range(1, 5):  # std_category_1, std_category_2, std_category_3, std_category_4
        if f'std_category_{i}' in value2 and value2[f'std_category_{i}'] not in [None, 'NULL', '']:
            row[f'std_category_{i}'] = value2[f'std_category_{i}']
    
    if any(f'std_category_{i}' in value2 and value2[f'std_category_{i}'] not in [None, 'NULL', ''] for i in range(1, 5)):
        category_src_parts.append(src2)
    
    # 3. 设置标准品类来源标识 - 中文分号拼接
    if category_src_parts:
        row['std_category_src'] = '；'.join(category_src_parts)
    # 如果没有规则集产生有效结果，保持上游的std_category_src字段不变
    
    # 标准品牌名清洗 - 用规则集104982进行修正（SQL已完成第一步修正）
    value, src = get_mapping_result_and_src(row, 'std_brand_name')
    if 'std_brand_name' in value and value['std_brand_name'] not in [None, 'NULL', '']:
        row['std_brand_name'] = value['std_brand_name']
        row['std_brand_name_src'] = src
    
    # 标准商品单元清洗 - std_spu_name不可为空，flavor可为空
    value, src = get_mapping_result_and_src(row, 'std_spu_name')
    if 'std_spu_name' in value and value['std_spu_name'] not in [None, 'NULL', '']:
        row['std_spu_name'] = value['std_spu_name']
        row['std_spu_name_src'] = src
    if 'flavor' in value and value['flavor'] not in [None, 'NULL', '']:
        row['flavor'] = value['flavor']
    
    # 产品属性清洗 - 依赖规则集104983，只保留有值的字段
    value, src = get_mapping_result_and_src(row, 'product_attributes')
    attribute_fields = ['form', 'need_toilet_brush', 'need_with_water', 'use_method', 
                       'spray_form', 'texture', 'benefit', 'target_user', 
                       'product_scenarios', 'disinfectant_claim']
    
    # 只保留有值的字段进行赋值
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


# 主查询SQL
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
       s.std_category_1,
       s.std_category_2,
       s.std_category_3,
       s.std_category_4,
       s.std_category_src,
       coalesce(
    if(
        (
            if(regexp_extract(s.std_brand_name,'[a-zA-Z]+',0) != '', (lower(concat(s.item_title, coalesce(s.pfsku_title,''), coalesce(s.brand_name,''))) 
             like lower(concat('%', regexp_replace(regexp_extract(s.std_brand_name,'[a-zA-Z]+',0),'[^0-9a-zA-Z\u4e00-\u9fa5]+',''), '%'))
            ), 1=2) or 
            if(regexp_extract(s.std_brand_name,'[\u4e00-\u9fa5]+',0) != '', (lower(concat(s.item_title, coalesce(s.pfsku_title,''), coalesce(s.brand_name,''))) 
             like lower(concat('%', regexp_replace(regexp_extract(s.std_brand_name,'[\u4e00-\u9fa5]+',0),'[^0-9a-zA-Z\u4e00-\u9fa5]+',''), '%'))
            ), 1=2) or 
            s.brand_name is null or 
            s.brand_name = '' or 
            lower(s.brand_name) like '%无品牌%' or 
            lower(s.brand_name) like '%其他%' or 
            lower(s.brand_name) like '%other%'
        )
        ,
        s.std_brand_name,
        s.brand_name
    ),
    s.brand_name
) as std_brand_name,
       if(
           s.std_brand_name != coalesce(
               if(
                   (
                       if(regexp_extract(s.std_brand_name,'[a-zA-Z]+',0) != '', (lower(concat(s.item_title, coalesce(s.pfsku_title,''), coalesce(s.brand_name,''))) 
                        like lower(concat('%', regexp_replace(regexp_extract(s.std_brand_name,'[a-zA-Z]+',0),'[^0-9a-zA-Z\u4e00-\u9fa5]+',''), '%'))
                       ), 1=2) or 
                       if(regexp_extract(s.std_brand_name,'[\u4e00-\u9fa5]+',0) != '', (lower(concat(s.item_title, coalesce(s.pfsku_title,''), coalesce(s.brand_name,''))) 
                        like lower(concat('%', regexp_replace(regexp_extract(s.std_brand_name,'[\u4e00-\u9fa5]+',0),'[^0-9a-zA-Z\u4e00-\u9fa5]+',''), '%'))
                       ), 1=2) or 
                       s.brand_name is null or 
                       s.brand_name = '' or 
                       lower(s.brand_name) like '%无品牌%' or 
                       lower(s.brand_name) like '%其他%' or 
                       lower(s.brand_name) like '%other%'
                   )
                   ,
                   s.std_brand_name,
                   s.brand_name
               ),
               s.brand_name
           ),
           '因相同标题或std_brand_name为空原因修正异常std_brand_name',
           s.std_brand_name_src
       ) as std_brand_name_src,
       s.std_spu_name,
       s.flavor,
       s.std_group_spu_name,
       s.is_bundle,
       s.is_gift,
       s.sku_no,
       s.bundle_name,
       s.std_spu_name_src,
       s.weight,
       s.weight_unit,
       s.package,
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
       null as attribute_src,
       s.sales,
       s.count,
       s.pfsku_discount_price,
       s.hash_id,
       s.month_dt
FROM {source_table} s
WHERE {month_condition}
'''

# Schema定义
SCHEMA = '''
  platform                    STRING,
  item_id                     BIGINT,
  pfsku_id                    BIGINT,
  item_title                  STRING,
  pfsku_title                 STRING,
  first_image                 STRING,
  pfsku_image                 STRING,
  category_name               STRING,
  shop_name                   STRING,
  brand_name                  STRING,
  item_url                    STRING,
  pfsku_url                   STRING,
  tags                        STRING,
  std_category_1              STRING,
  std_category_2              STRING,
  std_category_3              STRING,
  std_category_4              STRING,
  std_category_src            STRING,
  std_brand_name              STRING,
  std_spu_name                STRING,
  flavor                      STRING,
  std_group_spu_name          STRING,
  is_bundle                   INT,
  is_gift                     INT,
  sku_no                      INT,
  bundle_name                 STRING,
  std_brand_name_src          STRING,
  std_spu_name_src            STRING,
  weight                      DOUBLE,
  weight_unit                 STRING,
  package                     INT,
  package_unit                STRING,
  unit_weight                 DOUBLE,
  quantity                    INT,
  quantity_unit               STRING,
  form                        STRING,
  need_toilet_brush           STRING,
  need_with_water             STRING,
  use_method                  STRING,
  spray_form                  STRING,
  texture                     STRING,
  benefit                     STRING,
  target_user                 STRING,
  product_scenarios           STRING,
  disinfectant_claim          STRING,
  attribute_src               STRING,
  sales                       DOUBLE,
  count                       INT,
  pfsku_discount_price        DOUBLE,
  hash_id                     STRING,
  month_dt                    STRING
'''

# DDL定义 - 严格按照需求文档的字段顺序
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
    std_category_1 STRING,
    std_category_2 STRING,
    std_category_3 STRING,
    std_category_4 STRING,
    std_category_src STRING,
    std_brand_name STRING,
    std_spu_name STRING,
    flavor STRING,
    std_group_spu_name STRING,
    is_bundle INT,
    is_gift INT,
    sku_no INT,
    bundle_name STRING,
    std_brand_name_src STRING,
    std_spu_name_src STRING,
    weight DOUBLE,
    weight_unit STRING,
    package INT,
    package_unit STRING,
    unit_weight DOUBLE,
    quantity INT,
    quantity_unit STRING,
    form STRING,
    need_toilet_brush STRING,
    need_with_water STRING,
    use_method STRING,
    spray_form STRING,
    texture STRING,
    benefit STRING,
    target_user STRING,
    product_scenarios STRING,
    disinfectant_claim STRING,
    attribute_src STRING,
    sales DOUBLE,
    count INT,
    pfsku_discount_price DOUBLE,
    hash_id STRING
) PARTITIONED BY (month_dt STRING) 
COMMENT 'AIGC结果数据清洗表' 
STORED AS PARQUET
'''

# 执行数据转换
rdd = spark.sql(query).repartition(200).rdd.mapPartitions(apply_transformation)
df = spark.createDataFrame(rdd, SCHEMA)
df.repartition(2).createOrReplaceTempView('tv_dwd_aigc_result_data')

# 创建目标表
spark.sql(DDL)
spark.sql('SET spark.sql.parquet.compression.codec=snappy')
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

# 插入数据到目标表
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
       std_category_1,
       std_category_2,
       std_category_3,
       std_category_4,
       std_category_src,
       std_brand_name,
       std_spu_name,
       flavor,
       std_group_spu_name,
       is_bundle,
       is_gift,
       sku_no,
       bundle_name,
       std_brand_name_src,
       std_spu_name_src,
       weight,
       weight_unit,
       package,
       package_unit,
       unit_weight,
       quantity,
       quantity_unit,
       form,
       need_toilet_brush,
       need_with_water,
       use_method,
       spray_form,
       texture,
       benefit,
       target_user,
       product_scenarios,
       disinfectant_claim,
       attribute_src,
       sales,
       count,
       pfsku_discount_price,
       hash_id
       {month_field}
FROM tv_dwd_aigc_result_data
''')

spark.stop()

# Impala统计更新
impala.execute(f'invalidate metadata {result_table}')
impala.execute(drop_incremental_stats)
impala.execute(f'compute incremental stats {result_table}')
