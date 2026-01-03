import collections
import copy
import json

from collections import OrderedDict
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import when,col,sum,avg,count,concat,lit
from pigeon.connector import new_impala_connector
from ymrbdt.attributes import read_rule_set


spark = SparkSession.builder.appName("onework.{{dag_name}}.{{job_name}}.{{ execution_date }}").enableHiveSupport().getOrCreate()
impala = new_impala_connector()

update_mode = '{{ update_mode }}'
month_dt = '{{ month_dt }}'
project_start_month = '{{ project_start_month }}'

source_table = '{{ monthly_count }}'
result_table = '{{ monthly_product_status }}'

status_ruleset_url = 'https://onemap.yimian.com.cn/project-management/281/1746/info'
status_ruleset_id = 105001
status_ruleset = read_rule_set(status_ruleset_id)

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

def transform(row):
    # row['platform'] = row['platform'].lower()
    rv = status_ruleset.transform(row)[0]
    status = rv[status_ruleset.result_column[0]]
    if rv['__matched_rule'] == '__DEFAULT__':
        priority = '__DEFAULT__'
    else:
        priority = json.loads(rv['__matched_rule'])['priority']
    
    if status == '1':
        row['status'] = 'exclude'
        row['status_src'] = f'{status_ruleset_id},{priority}'

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
       c.shop_name,
       c.category_id,
       c.brand_id,
       CAST(NULL AS STRING) as status,
       CAST(NULL AS STRING) as status_src,
       a.month_dt
FROM {source_table} a
LEFT JOIN (SELECT * FROM {{ std_mapping }} a WHERE {month_condition} AND sku_no = 1) c ON a.month_dt = c.month_dt
AND lower(a.platform) = lower(c.platform)
AND a.item_id = c.item_id
AND a.pfsku_id = c.pfsku_id
AND (lower(a.platform) != 'amazon' OR a.market = c.market)
AND IF(lower(a.platform) = 'suning', a.shop_id, 0) = IF(lower(c.platform) = 'suning', c.shop_id, 0) 
WHERE {month_condition} 
'''

SCHEMA = '''
    platform                STRING,
    market                  STRING,
    item_id                 STRING,
    pfsku_id                STRING,
    shop_id                 STRING,
    status                  STRING,
    status_src              STRING,
    month_dt                STRING
'''

DDL = f'''
CREATE TABLE IF NOT EXISTS {result_table} (
    platform                STRING,
    market                  STRING,
    item_id                 STRING,
    pfsku_id                STRING,
    shop_id                 STRING,
    status                  STRING,
    status_src              STRING
) 
PARTITIONED BY (month_dt STRING)
STORED AS PARQUET
'''

rdd = spark.sql(query).repartition(200).rdd.mapPartitions(apply_transformation)
df = spark.createDataFrame(rdd, SCHEMA)

df = df.filter("status is not null")
df.repartition(1).createOrReplaceTempView('tv_monthly_product_status')

spark.sql(DDL)
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql(f'''
INSERT OVERWRITE TABLE {result_table} PARTITION({month_partition})
SELECT platform,
       market,
       item_id,
       pfsku_id,
       shop_id,
       status,
       status_src
       {month_field}
FROM tv_monthly_product_status
''')
spark.stop()


impala.execute(f'{{ refresh }}  {result_table} PARTITION({month_partition})') 
impala.execute(drop_incremental_stats) 
impala.execute(f'compute incremental stats {result_table} PARTITION({month_partition})')
