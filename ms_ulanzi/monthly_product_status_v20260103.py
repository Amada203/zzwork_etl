# 多分区增量全量刷数版本 - 20260103
import collections
import json

from collections import OrderedDict
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import when,col,sum,avg,count,concat,lit
from pigeon.connector import new_impala_connector
from ymrbdt.attributes import read_rule_set


spark = SparkSession.builder.appName("onework.{{dag_name}}.{{job_name}}.{{ execution_date }}").enableHiveSupport().getOrCreate()
impala = new_impala_connector()

update_mode = '{{ update_mode }}'
CALC_PARTITION = ['{{ month_dt }}', '{{ platform_filter }}', '{{ market_filter }}']
project_start_month = '{{ project_start_month }}'

source_table = '{{ monthly_count }}'
result_table = '{{ monthly_product_status }}'

status_ruleset_id = 105001
status_ruleset = read_rule_set(status_ruleset_id)


def _normalize_filter(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value or value.upper() in ('*', 'ALL', 'NULL', 'NONE'):
            return None
        return value
    return value


# 解析分区参数
if isinstance(CALC_PARTITION, list):
    month_dt = _normalize_filter(CALC_PARTITION[0] if len(CALC_PARTITION) > 0 else None)
    platform_filter = _normalize_filter(CALC_PARTITION[1] if len(CALC_PARTITION) > 1 else None)
    market_filter = _normalize_filter(CALC_PARTITION[2] if len(CALC_PARTITION) > 2 else None)
else:
    month_dt = _normalize_filter(CALC_PARTITION)
    platform_filter = None
    market_filter = None

if update_mode == 'incremental':
    where_conditions = []
    if month_dt:
        where_conditions.append(f'a.month_dt = "{month_dt}"')
    if platform_filter:
        where_conditions.append(f'a.platform = "{platform_filter}"')
    if market_filter:
        where_conditions.append(f'a.market = "{market_filter}"')
    month_condition = ' AND '.join(where_conditions) if where_conditions else '1=1'
    
    partition_parts = []
    if platform_filter:
        partition_parts.append(f'platform = "{platform_filter}"')
    if month_dt:
        partition_parts.append(f'month_dt = "{month_dt}"')
    if market_filter:
        partition_parts.append(f'market = "{market_filter}"')
    month_partition = ','.join(partition_parts) if partition_parts else 'platform,month_dt,market'
    drop_incremental_stats = f'DROP INCREMENTAL STATS {result_table} PARTITION({month_partition});'
elif update_mode == 'full':
    where_conditions = []
    if month_dt:
        where_conditions.append(f'a.month_dt >= "{project_start_month}"')
    else:
        where_conditions.append('a.month_dt IS NOT NULL')
    if platform_filter:
        where_conditions.append(f'a.platform = "{platform_filter}"')
    if market_filter:
        where_conditions.append(f'a.market = "{market_filter}"')
    month_condition = ' AND '.join(where_conditions) if where_conditions else '1=1'
    
    partition_parts = []
    if platform_filter:
        partition_parts.append(f'platform = "{platform_filter}"')
    if month_dt:
        partition_parts.append(f'month_dt >= "{project_start_month}"')
    if market_filter:
        partition_parts.append(f'market = "{market_filter}"')
    month_partition = 'platform,month_dt,market'
    drop_incremental_stats = f'DROP INCREMENTAL STATS {result_table} PARTITION(platform,month_dt,market);'
else:
    raise ValueError(f'update_mode="{update_mode}" is invalid !!!')

def transform(row):
    exclude_flag = row.get('exclude_flag', 0)
    
    if exclude_flag == 1:
        row['status'] = 'exclude'
        row['status_src'] = 'dwd_exclude_scope'
    else:
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


if update_mode == 'incremental':
    exclude_where_clause = f'month_dt = "{month_dt}"'
else:
    exclude_where_clause = f'month_dt >= "{project_start_month}"'

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
       COALESCE(e.exclude, 0) as exclude_flag,
       a.month_dt
FROM {source_table} a
LEFT JOIN (SELECT * FROM {{ std_mapping }} a WHERE {month_condition} AND sku_no = 1) c ON a.month_dt = c.month_dt
AND lower(a.platform) = lower(c.platform)
AND a.item_id = c.item_id
AND a.pfsku_id = c.pfsku_id
AND (lower(a.platform) != 'amazon' OR a.market = c.market)
AND IF(lower(a.platform) = 'suning', a.shop_id, 0) = IF(lower(c.platform) = 'suning', c.shop_id, 0)
LEFT JOIN (SELECT * FROM {{dwd_exclude_scope}} WHERE {exclude_where_clause} AND exclude = 1) e 
ON lower(a.platform) = lower(e.platform)
AND a.item_id = e.item_id
AND a.month_dt = e.month_dt
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
    exclude_flag            INT,
    month_dt                STRING
'''

DDL = f'''
CREATE TABLE IF NOT EXISTS {result_table} (
    item_id                 STRING,
    pfsku_id                STRING,
    shop_id                 STRING,
    status                  STRING,
    status_src              STRING
) 
PARTITIONED BY (platform STRING, month_dt STRING, market STRING)
STORED AS PARQUET
'''

rdd = spark.sql(query).repartition(200).rdd.mapPartitions(apply_transformation)
df = spark.createDataFrame(rdd, SCHEMA)

df = df.filter("status is not null")
df.repartition(1).createOrReplaceTempView('tv_monthly_product_status')

spark.sql(DDL)
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql(f'''
INSERT OVERWRITE TABLE {result_table} PARTITION(platform,month_dt,market)
SELECT item_id,
       pfsku_id,
       shop_id,
       status,
       status_src,
       platform,
       month_dt,
       market
FROM tv_monthly_product_status
''')

# 如果分区条件不完整（少于3个分区字段），查询实际受影响的分区组合
partition_list = None
if update_mode == 'incremental' and len(partition_parts) < 3:
    # 构建过滤条件查询实际分区组合
    filter_conditions = []
    if platform_filter:
        filter_conditions.append(f'platform = "{platform_filter}"')
    if month_dt:
        filter_conditions.append(f'month_dt = "{month_dt}"')
    if market_filter:
        filter_conditions.append(f'market = "{market_filter}"')
    
    partition_df = df.select('platform', 'month_dt', 'market').distinct()
    if filter_conditions:
        partition_df = partition_df.filter(' AND '.join(filter_conditions))
    
    partition_list = [(row.platform, row.month_dt, row.market) for row in partition_df.collect()]

spark.stop()

# 刷新分区元数据：如果分区条件完整则直接刷新，否则逐个刷新受影响的分区
if partition_list is None:
    impala.execute(f'{{ refresh }} {result_table} PARTITION({month_partition})')
    impala.execute(drop_incremental_stats)
    impala.execute(f'compute incremental stats {result_table} PARTITION({month_partition})')
else:
    for platform, month_dt, market in partition_list:
        partition_clause = f'platform="{platform}",month_dt="{month_dt}",market="{market}"'
        drop_stats_sql = f'DROP INCREMENTAL STATS {result_table} PARTITION({partition_clause});'
        impala.execute(f'{{ refresh }} {result_table} PARTITION({partition_clause})')
        impala.execute(drop_stats_sql)
        impala.execute(f'compute incremental stats {result_table} PARTITION({partition_clause})')

