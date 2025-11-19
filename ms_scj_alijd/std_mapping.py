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

source_table = '{{ std_mapping_origin }}'
result_table = '{{ std_mapping }}'

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


project_id = 281
rule_set_dict = {
    'std_category_name': (104961, read_rule_set(104961),),
    'fix_std_brand_name': (104962, read_rule_set(104962),),
    'std_brand_name': (104840, read_rule_set(104840),),
    'sub_channel': (104950, read_rule_set(104950),),
    'manufacturer': (104952, read_rule_set(104952),),
    'package_weight': (104963, read_rule_set(104963),),
    'std_sub_segment': (104956, read_rule_set(104956),),
    'std_scj_category': (104969, read_rule_set(104969),),
    'form': (104964, read_rule_set(104964),),
    'std_disinfectant_claim': (104968, read_rule_set(104968),),
    'target_user': (104965, read_rule_set(104965),),
    'target_location': (104966, read_rule_set(104966),),
    'target_format': (104967, read_rule_set(104967),),
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
    row.setdefault('channel', None)
    row.setdefault('std_segment', None)
    row.setdefault('std_scj_category', None)
    row.setdefault('std_scj_segment', None)
    row.setdefault('std_scj_sub_segment', None)
    row.setdefault('std_disinfectant_claim', None)
    row.setdefault('target_user', None)
    row.setdefault('target_location', None)
    row.setdefault('total_size', None)
    
    # 5. 清洗package\weight字段，取值优先级：规则104963>上游std_mapping_origin表
    # 21. size取值优先级：规则104963>上游std_mapping_origin表
    value, src = get_mapping_result_and_src(row, 'package_weight')
    if 'package' in value and value['package']:
        row['package'] = safe_int(value['package'])
        row['package_weight_src'] = src
    if 'weight' in value and value['weight']:
        row['weight'] = safe_float(value['weight'])
        row['package_weight_src'] = src
    if  ( 'weight' in value and (value['weight'] ) )or ('package' in value and value['package']):
        row['total_weight'] = row['weight'] * row['package']
        if '__DEFAULT__' not in src:
            row['total_weight_src'] = src    
    if 'size' in value and value['size']:
        row['size'] = safe_float(value['size'])
        row['size_src'] = src
    # 拼接 std_sku_name
    weight_unit = row.get('weight_unit', 'ml')
    if not row.get('std_spu_name') or row.get('std_spu_name', '').lower() == 'others' or row.get('weight') is None:
        row['std_sku_name'] = None
    else:
        row['std_sku_name'] = f"{row['std_spu_name']}{safe_round(row['weight'])}{weight_unit}"
    
    # sku_title 字段清洗逻辑
    # 1. 优先取标注得到的标准商品名称 (std_sku_name = std_spu_name + weight + weight单位)
    if row.get('std_sku_name'):
        row['sku_title'] = row['std_sku_name']
    else:
        # 2. 无标注数据，则取值逻辑为：Tmall&JD平台调整为 sku_title = pfsku_title+ item_title，其余平台 sku_title = item_title
        platform = row.get('platform', '').lower()
        if platform in ['tmall', 'jd']:
            pfsku_title = row.get('pfsku_title', '')
            item_title = row.get('item_title', '')
            row['sku_title'] = f"{pfsku_title}{item_title}" if pfsku_title and item_title else (pfsku_title or item_title)
        else:
            row['sku_title'] = row.get('item_title', '') 

    # 10. std_sub_segment取值优先级：规则104956>上游std_mapping_origin表
    value, src = get_mapping_result_and_src(row, 'std_sub_segment')
    if 'fix_std_sub_segment' in value and value['fix_std_sub_segment']:
        row['std_sub_segment'] = value['fix_std_sub_segment']
        row['std_sub_segment_src'] = src
    
    # 1. 清洗std_category_name、std_segment字段，规则映射：规则104961（输入：std_sub_segment）
    value, src = get_mapping_result_and_src(row, 'std_category_name')
    if 'std_category_name' in value and value['std_category_name']:
        row['std_category_name'] = value['std_category_name']
        row['std_category_name_src'] = src
    if 'std_segment' in value and value['std_segment']:
        row['std_segment'] = value['std_segment']
        row['std_segment_src'] = src
    
    # 2. 清洗std_brand_name字段，调用优先级：规则104962（最高优先级）>规则104840>上游std_mapping_origin表
    # 优先尝试规则104962
    value, src = get_mapping_result_and_src(row, 'fix_std_brand_name')
    if 'fix_std_brand_name' in value and value['fix_std_brand_name']:
        row['std_brand_name'] = value['fix_std_brand_name']
        row['std_brand_name_src'] = src
    else:
        # 尝试规则104840
        value, src = get_mapping_result_and_src(row, 'std_brand_name')
        if 'std_brand_name' in value and value['std_brand_name']:
            row['std_brand_name'] = value['std_brand_name']
            row['std_brand_name_src'] = src
    
    # 8. 清洗channel字段，规则映射：规则104950
    value, src = get_mapping_result_and_src(row, 'sub_channel')
    if 'sub_channel' in value and value['sub_channel']:
        row['media'] = value['sub_channel']
        row['media_src'] = src
    if 'channel' in value and value['channel']:
        row['channel'] = value['channel']
        row['channel_src'] = src
    

    # 15-16. std_brand_en, std_sub_brand_en规则映射：规则104952
    value, src = get_mapping_result_and_src(row, 'manufacturer')
    if 'manufacturer' in value and value['manufacturer']:
        row['manufacturer'] = value['manufacturer']
        row['manufacturer_src'] = src
    if 'std_brand_en' in value and value['std_brand_en']:
        row['std_brand_en'] = value['std_brand_en']
        row['std_brand_en_src'] = src
    if 'std_sub_brand_en' in value and value['std_sub_brand_en']:
        row['std_sub_brand_en'] = value['std_sub_brand_en']
        row['std_sub_brand_en_src'] = src
    
    # 20. target_format取值优先级：规则104967>上游std_mapping_origin表
    value, src = get_mapping_result_and_src(row, 'target_format')
    if 'fix_format' in value and value['fix_format']:
        row['target_format'] = value['fix_format']
        row['target_format_src'] = src


    
    # 11-13. std_scj_category, std_scj_segment, std_scj_sub_segment规则映射：规则104969
    value, src = get_mapping_result_and_src(row, 'std_scj_category')
    if 'std_scj_category' in value and value['std_scj_category']:
        row['std_scj_category'] = value['std_scj_category']
        row['std_scj_category_src'] = src
    if 'std_scj_segment' in value and value['std_scj_segment']:
        row['std_scj_segment'] = value['std_scj_segment']
        row['std_scj_segment_src'] = src
    if 'std_scj_sub_segment' in value and value['std_scj_sub_segment']:
        row['std_scj_sub_segment'] = value['std_scj_sub_segment']
        row['std_scj_sub_segment_src'] = src
    
    # 14. form取值优先级：规则104964>上游std_mapping_origin表
    value, src = get_mapping_result_and_src(row, 'form')
    if 'fix_form' in value and value['fix_form']:
        row['form'] = value['fix_form']
        row['form_src'] = src
    
    
    # 17. std_disinfectant_claim规则映射：规则104968
    value, src = get_mapping_result_and_src(row, 'std_disinfectant_claim')
    if 'std_disinfectant_claim' in value and value['std_disinfectant_claim']:
        row['std_disinfectant_claim'] = value['std_disinfectant_claim']
        row['std_disinfectant_claim_src'] = src
    
    # 18. target_user规则映射：规则104965
    value, src = get_mapping_result_and_src(row, 'target_user')
    if 'target_user' in value and value['target_user']:
        row['target_user'] = value['target_user']
        row['target_user_src'] = src
    
    # 19. target_location规则映射：规则104966
    value, src = get_mapping_result_and_src(row, 'target_location')
    if 'target_location' in value and value['target_location']:
        row['target_location'] = value['target_location']
        row['target_location_src'] = src
    
    # 22. total_size取值逻辑：total_size = size * package
    if row.get('size') and row.get('package'):
        row['total_size'] = row['size'] * row['package']
    else:
        row['total_size'] = None
    
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
SELECT s.unique_id,
       s.platform,
       s.market,
       s.item_id,
       s.pfsku_id,
       s.item_title,
       s.pfsku_title,
       s.category_1,
       s.category_2,
       s.category_3,
       s.category_4,
       s.category_5,
       s.category_6,
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
       s.package,
       s.weight,
       'ml' as weight_unit,
       s.total_weight,
       s.total_weight_src,
       s.attributes,
       s.sku_src,
       s.media,
       s.std_category_name,
       s.std_sub_category_name,
       s.std_brand_name,
       s.manufacturer,
       s.variant,
       s.std_spu_name,
       s.std_sku_name,
       s.media_src,
       s.std_category_name_src,
       s.std_sub_category_name_src,
       s.std_brand_name_src,
       s.manufacturer_src,
       s.variant_src,
       s.std_spu_name_src,
       s.std_sub_segment,
       s.std_sub_segment_src,
       s.form,
       s.form_src,
       s.target_format,
       s.target_format_src,
       s.quantity as size,
       s.month_dt
FROM {source_table} s
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
  weight_unit                 STRING,
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
  -- 新增字段
  std_segment                 STRING,
  std_segment_src             STRING,
  sub_channel                 STRING,
  sub_channel_src             STRING,
  package_weight_src          STRING,
  channel                     STRING,
  channel_src                 STRING,
  std_sub_segment             STRING,
  std_sub_segment_src         STRING,
  std_scj_category            STRING,
  std_scj_category_src        STRING,
  std_scj_segment             STRING,
  std_scj_segment_src         STRING,
  std_scj_sub_segment         STRING,
  std_scj_sub_segment_src     STRING,
  form                        STRING,
  form_src                    STRING,
  std_brand_en                STRING,
  std_brand_en_src            STRING,
  std_sub_brand_en            STRING,
  std_sub_brand_en_src        STRING,
  std_disinfectant_claim      STRING,
  std_disinfectant_claim_src  STRING,
  target_user                 STRING,
  target_user_src             STRING,
  target_location             STRING,
  target_location_src         STRING,
  target_format               STRING,
  target_format_src           STRING,
  size                        DOUBLE,
  size_src                    STRING,
  total_size                  DOUBLE,
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
  weight_unit                 STRING,
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
  -- 新增字段
  std_segment                 STRING,
  std_segment_src             STRING,
  sub_channel                 STRING,
  sub_channel_src             STRING,
  package_weight_src          STRING,
  channel                     STRING,
  channel_src                 STRING,
  std_sub_segment             STRING,
  std_sub_segment_src         STRING,
  std_scj_category            STRING,
  std_scj_category_src        STRING,
  std_scj_segment             STRING,
  std_scj_segment_src         STRING,
  std_scj_sub_segment         STRING,
  std_scj_sub_segment_src     STRING,
  form                        STRING,
  form_src                    STRING,
  std_brand_en                STRING,
  std_brand_en_src            STRING,
  std_sub_brand_en            STRING,
  std_sub_brand_en_src        STRING,
  std_disinfectant_claim      STRING,
  std_disinfectant_claim_src  STRING,
  target_user                 STRING,
  target_user_src             STRING,
  target_location             STRING,
  target_location_src         STRING,
  target_format               STRING,
  target_format_src           STRING,
  size                        DOUBLE,
  size_src                    STRING,
  total_size                  DOUBLE
) PARTITIONED BY (month_dt STRING) 
COMMENT 'OneMap标准字段表' 
STORED AS PARQUET
'''

rdd = spark.sql(query).repartition(400).rdd.mapPartitions(apply_transformation)
df = spark.createDataFrame(rdd, SCHEMA)
df.repartition(2).createOrReplaceTempView('tv_std_mapping')

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
       package,
       weight,
       weight_unit,
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
       -- 新增字段
       std_segment,
       std_segment_src,
       sub_channel,
       sub_channel_src,
       package_weight_src,
       channel,
       channel_src,
       std_sub_segment,
       std_sub_segment_src,
       std_scj_category,
       std_scj_category_src,
       std_scj_segment,
       std_scj_segment_src,
       std_scj_sub_segment,
       std_scj_sub_segment_src,
       form,
       form_src,
       std_brand_en,
       std_brand_en_src,
       std_sub_brand_en,
       std_sub_brand_en_src,
       std_disinfectant_claim,
       std_disinfectant_claim_src,
       target_user,
       target_user_src,
       target_location,
       target_location_src,
       target_format,
       target_format_src,
       size,
       size_src,
       total_size
       {month_field}
FROM tv_std_mapping
''')
spark.stop()


impala.execute(f'{{ refresh }} {result_table}')
impala.execute(drop_incremental_stats)
impala.execute(f'compute incremental stats {result_table}')
