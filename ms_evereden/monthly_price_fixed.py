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

source_table = '{{ monthly_price }}'
result_table = '{{ monthly_price_fixed }}'
feigua_table = '{{ feigua_monthly_sales }}'

# price_ruleset_url = 'https://onemap.yimian.com.cn/project-management/281/1725/info'
price_ruleset_id = 104864
price_ruleset = read_rule_set(price_ruleset_id)

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


def transform(row):
    # 优先级1: 104864 fix price mapping
    xrow = {
        'month_dt': row['month_dt'],
        'platform': row['platform'],
        'item_id': row['item_id'],
        'pfsku_id': row['pfsku_id'],
    }
    rv = price_ruleset.transform(xrow)[0]
    price = rv[price_ruleset.result_column[0]]
    if rv['__matched_rule'] == '__DEFAULT__':
        priority = '__DEFAULT__'
    else:
        priority = json.loads(rv['__matched_rule'])['priority']
        
    if price is not None:
        row['page_price'] = float(price)
        row['discount_price'] = float(price)
        row['discount_price_src'] = f'{price_ruleset_id},{priority}'
    return row


def _spark_row_to_ordereddict(row):
    return collections.OrderedDict(zip(row.__fields__, row))


def apply_transformation(rows):
    for row in rows:
        yield transform(_spark_row_to_ordereddict(row))


query = f'''
SELECT mp.platform,
       mp.item_id,
       mp.pfsku_id,
       mp.shop_id,
       COALESCE(fs.price, mp.page_price) as page_price,
       COALESCE(fs.price, mp.discount_price) as discount_price,
       case when fs.price is not null then 'feigua_monthly_sales' else mp.discount_price_src end as discount_price_src,
       mp.legacy_page_price,
       mp.legacy_discount_price,
       mp.month_dt
FROM {source_table} mp
LEFT JOIN {feigua_table} fs 
  ON mp.platform = fs.platform 
  AND CAST(mp.item_id AS STRING) = fs.item_id 
  AND mp.month_dt = fs.month_dt
WHERE {month_condition} 
'''

SCHEMA = '''
    platform                    STRING,
    item_id                     BIGINT,
    pfsku_id                    BIGINT,
    shop_id                     STRING,
    page_price                  DOUBLE,
    discount_price              DOUBLE,
    discount_price_src          STRING,
    legacy_page_price           DOUBLE,
    legacy_discount_price       DOUBLE,
    month_dt                    STRING
'''

DDL = f'''
CREATE TABLE IF NOT EXISTS {result_table} (
    platform                    STRING,
    item_id                     bigint,
    pfsku_id                    bigint,
    shop_id                     STRING,    
    page_price                  DOUBLE,
    discount_price              DOUBLE,
    discount_price_src          STRING,
    legacy_page_price           DOUBLE,
    legacy_discount_price       DOUBLE
) COMMENT '月度价格 (OneMap 修复后)'
PARTITIONED BY (month_dt STRING)
STORED AS PARQUET
'''

rdd = spark.sql(query).repartition(200).rdd.mapPartitions(apply_transformation)
df = spark.createDataFrame(rdd, SCHEMA)
df.repartition(2).createOrReplaceTempView('tv_monthly_price_fixed')

spark.sql(DDL)
spark.sql('SET spark.sql.parquet.compression.codec=gzip')
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql(f'''
INSERT OVERWRITE TABLE {result_table} PARTITION({month_partition})
SELECT platform,
       item_id,
       pfsku_id,
       shop_id,
       page_price,
       discount_price,
       discount_price_src,
       legacy_page_price,
       legacy_discount_price
       {month_field}
FROM tv_monthly_price_fixed
''')
spark.stop()


impala.execute(f'{{ refresh }} {result_table}')
impala.execute(drop_incremental_stats)
impala.execute(f'compute incremental stats {result_table}')
