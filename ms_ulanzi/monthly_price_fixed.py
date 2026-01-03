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

price_ruleset_url = 'https://onemap.yimian.com.cn/project-management/281/1725/info'
price_ruleset_id = 104999
price_ruleset = read_rule_set(price_ruleset_id)

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
    # xrow = {
    #     'month_dt': row['month_dt'],
    #     'platform': row['platform'],
    #     'item_id': row['item_id'],
    #     'pfsku_id': row['pfsku_id'],
    #     'shop_id': row['shop_id'],
    # }
    rv = price_ruleset.transform(row)[0]
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
SELECT a.platform,
       a.market,
       a.item_id,
       a.pfsku_id,
       c.shop_id ,
       a.page_price,
       a.discount_price,
       a.discount_price_src,
       a.legacy_page_price,
       a.legacy_discount_price,
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
    platform                    STRING,
    market                      STRING,
    item_id                     STRING,
    pfsku_id                    STRING,
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
    market                      STRING,
    item_id                     STRING,
    pfsku_id                    STRING,
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
/* +NOSHUFFLE,NOCLUSTERED */
SELECT platform,
       market,
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


impala.execute(f'{{ refresh }} {result_table} PARTITION({month_partition})')
impala.execute(drop_incremental_stats)
impala.execute(f'compute incremental stats {result_table} PARTITION({month_partition})')
