import collections
import copy
import json

from collections import OrderedDict
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import when,col,sum,avg,count,concat,lit,lower
from pigeon.connector import new_impala_connector
from ymrbdt.attributes import read_rule_set


spark = SparkSession.builder.appName("onework.{{dag_name}}.{{job_name}}.{{ execution_date }}").enableHiveSupport().getOrCreate()
impala = new_impala_connector()

update_mode = '{{ update_mode }}'
month_dt = '{{ month_dt }}'
project_start_month = '{{ project_start_month }}'

source_table = '{{ monthly_count }}'
result_table = '{{ monthly_count_fixed }}'

# count_ruleset_url = 'https://onemap.yimian.com.cn/project-management/281/1724/info'
count_ruleset_id = 105000
count_ruleset = read_rule_set(count_ruleset_id)

count_ratio_ruleset_id = 105074
count_ratio_ruleset = read_rule_set(count_ratio_ruleset_id)

# 增量更新 或者 全量更新
if update_mode == 'incremental':
    month_field = ''
    month_condition = f'a.month_dt = "{month_dt}"'
    month_partition = f'month_dt = "{month_dt}"'
    drop_incremental_stats = f'DROP INCREMENTAL STATS {result_table} PARTITION(month_dt="{month_dt}");'
elif update_mode == 'full':
    month_field = ',month_dt'
    month_condition = '1=1'
    month_partition = 'month_dt'
    drop_incremental_stats = f'DROP INCREMENTAL STATS {result_table} PARTITION(month_dt>="{project_start_month}");'
else:
    raise ValueError(f'update_mode="{update_mode}" is invalid !!!')

def safe_int(s):
    if s is None:
        return s
    return int(s)  

def safe_float(s):
    if s is None:
        return s
    return float(s)

def transform(row):
    # row['platform'] = row['platform'].lower()
    rv = count_ruleset.transform(row)[0]
    count = rv[count_ruleset.result_column[0]]
    if rv['__matched_rule'] == '__DEFAULT__':
        priority = '__DEFAULT__'
    else:
        priority = json.loads(rv['__matched_rule'])['priority']
    
    if count is not None:
        row['pfsku_unit_sales'] = safe_int(count)  
        row['pfsku_unit_sales_src'] = f'{count_ruleset_id},{priority}'

    # 第二步：无论fix_count是否生效，都继续执行fix_count_ratio
    rv_ratio = count_ratio_ruleset.transform(row)[0]
    count_ratio = rv_ratio[count_ratio_ruleset.result_column[0]]
    if rv_ratio['__matched_rule'] == '__DEFAULT__':
        priority = '__DEFAULT__'  
    else:
        priority = json.loads(rv_ratio['__matched_rule'])['priority']
    if count_ratio is not None:
        base_sales = row.get('pfsku_unit_sales')
        if base_sales is not None:
            calculated_sales = safe_float(count_ratio) * safe_float(base_sales)
            row['pfsku_unit_sales'] = safe_int(calculated_sales)
            row['pfsku_unit_sales_src'] = f'{count_ratio_ruleset_id},{priority}'  
        row['fix_count_ratio'] = safe_float(count_ratio)

    return row

def _spark_row_to_ordereddict(row):
    return collections.OrderedDict(zip(row.__fields__, row))


def apply_transformation(rows):
    for row in rows:
        yield transform(_spark_row_to_ordereddict(row))


query = f'''
SELECT a.platform,
       a.market,
       a.item_id,
       a.pfsku_id,
       c.shop_id,
       a.price,
       a.item_unit_sales,
       a.pfsku_unit_sales,
       a.pfsku_unit_sales_src,
       a.legacy_item_unit_sales,
       a.legacy_pfsku_unit_sales,
       a.is_multi_pfsku,
       c.category_id,
       c.category_name,
       c.sub_category,
       c.brand_name,
       c.std_brand_name,
       CAST(NULL AS DOUBLE) AS fix_count_ratio,
       a.month_dt
FROM {source_table} a
LEFT JOIN (SELECT * FROM {{ std_mapping }} a WHERE {month_condition} AND sku_no = 1) c ON a.month_dt = c.month_dt
AND lower(a.platform) = lower(c.platform)
AND a.item_id = c.item_id
AND a.pfsku_id = c.pfsku_id
AND (lower(a.platform) != 'amazon' OR a.market = c.market)
AND IF(lower(a.platform) = 'suning', a.shop_id, '0') = IF(lower(c.platform) = 'suning', c.shop_id, '0') 
WHERE {month_condition} 
'''

SCHEMA = '''
    platform                STRING,
    market                  STRING,
    item_id                 STRING,
    pfsku_id                STRING,
    shop_id                 STRING,
    price                   double,
    item_unit_sales         BIGINT,
    pfsku_unit_sales        BIGINT,
    pfsku_unit_sales_src    STRING,
    legacy_item_unit_sales  BIGINT,
    legacy_pfsku_unit_sales BIGINT,
    is_multi_pfsku          STRING,
    category_id             BIGINT, 
    category_name           STRING,
    sub_category            STRING,
    brand_name              STRING,
    std_brand_name          STRING,
    fix_count_ratio         DOUBLE,
    month_dt                STRING
'''

DDL = f'''
CREATE TABLE IF NOT EXISTS {result_table} (
    platform                STRING,
    market                  STRING,
    item_id                 STRING,
    pfsku_id                STRING,
    shop_id                 STRING,
    price                   double,
    item_unit_sales         BIGINT,
    pfsku_unit_sales        BIGINT,
    pfsku_unit_sales_src    STRING,
    legacy_item_unit_sales  BIGINT,
    legacy_pfsku_unit_sales BIGINT,
    is_multi_pfsku          STRING,
    fix_count_ratio         DOUBLE
) COMMENT '月度销量 (OneMap 修复后)'
PARTITIONED BY (month_dt STRING)
STORED AS PARQUET
'''

rdd = spark.sql(query).repartition(200).rdd.mapPartitions(apply_transformation)
df = spark.createDataFrame(rdd, SCHEMA)


# 重新聚合计算product_unit_sales字段
# 只有suning平台shop_id才是主键，其他平台不是
df = df.withColumn("effective_shop_id", 
                   when(lower(col('platform')) == 'suning', col('shop_id')).otherwise('0'))

w_product = Window.partitionBy([
    'month_dt', 
    'platform', 
    'market', 
    'item_id', 
    'effective_shop_id'
])
df = df.withColumn("item_unit_sales", sum(col("pfsku_unit_sales")).over(w_product))


# 移除临时列
df = df.drop('effective_shop_id')
df.repartition(4).createOrReplaceTempView('tv_monthly_count_fixed')


spark.sql(DDL)
spark.sql('SET spark.sql.parquet.compression.codec=gzip')
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql(f'''
INSERT OVERWRITE TABLE {result_table} PARTITION({month_partition})
SELECT platform,
       market,
       item_id,
       pfsku_id,
       shop_id,
       price,
       item_unit_sales,
       pfsku_unit_sales,
       pfsku_unit_sales_src,
       legacy_item_unit_sales,
       legacy_pfsku_unit_sales,
       is_multi_pfsku,
       fix_count_ratio
       {month_field}
FROM tv_monthly_count_fixed
''')


spark.stop()
impala.execute(f'{{ refresh }} {result_table} PARTITION({month_partition})') 
impala.execute(drop_incremental_stats)
impala.execute(f'compute incremental stats {result_table} PARTITION({month_partition})')