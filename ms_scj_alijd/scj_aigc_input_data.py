# -*- coding: utf-8 -*-

import hashlib
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when, isnan, isnull, sum as spark_sum, row_number, lag
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.window import Window
from pigeon.connector import new_impala_connector


spark = SparkSession.builder.appName("onework.{{dag_name}}.{{job_name}}.{{ execution_date }}").enableHiveSupport().getOrCreate()
impala = new_impala_connector()

# 源表和目标表
source_table_1 = 'ms_scj_alijd.monthly_sales_wide'
source_table_2 = 'ms_scj_alijd.std_mapping'
result_table = 'ms_scj_alijd.scj_aigc_input_data'

# 增量全量模式配置
update_mode = '{{ update_mode }}'
month_dt = '{{ month_dt }}'
project_start_month = '{{ project_start_month }}'

# 增量更新 或者 全量更新
if update_mode == 'incremental':
    month_condition = f't0.month_dt = "{month_dt}"'
    month_partition = f'month_dt = "{month_dt}"'  # 分区子句
    drop_incremental_stats = f'DROP INCREMENTAL STATS {result_table} PARTITION(month_dt="{month_dt}");'
elif update_mode == 'full':
    month_condition = '1=1'
    month_partition = 'month_dt'  # 动态分区
    drop_incremental_stats = f'DROP INCREMENTAL STATS {result_table} PARTITION(month_dt>="{project_start_month}");'
else:
    raise ValueError(f'update_mode="{update_mode}" is invalid !!!')


# Hash函数定义
def generate_text_hash_id(item_title: Optional[str], pfsku_title: Optional[str]) -> str:
    """
    生成hash ID
    需要strip一下，原始标题有制表符，还有换行及多个空格
    
    Args:
        item_title: 商品标题，可以为None、NaN或其他类型
        pfsku_title: SKU标题，可以为None、NaN或其他类型
        
    Returns:
        hash ID字符串 (32位MD5)
    """
    import pandas as pd
    
    # 处理各种类型的输入（None、NaN、float等）
    def safe_str_strip(value):
        if value is None or pd.isna(value):
            return ""
        return str(value).strip()
    
    # 将换行及多个空格替换为单个空格
    item_title = ' '.join(safe_str_strip(item_title).split())
    pfsku_title = ' '.join(safe_str_strip(pfsku_title).split())
    
    # 组合字符串并生成MD5 hash
    combined = f"{item_title}_{pfsku_title}"
    return hashlib.md5(combined.encode('utf-8')).hexdigest()


# 目标表DDL
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
    pfsku_value_sales DOUBLE,
    pfsku_unit_sales INT,
    pfsku_discount_price DOUBLE,
    top80 DOUBLE,
    month_start STRING,
    text_hash_id STRING
) PARTITIONED BY (month_dt STRING) STORED AS PARQUET
'''


# 注册UDF函数
generate_text_hash_id_udf = udf(generate_text_hash_id, StringType())

# 基础数据查询 - 包含所有过滤条件，添加去重逻辑
base_query = f'''
SELECT DISTINCT
    t0.platform,
    t0.item_id,
    t0.pfsku_id,
    t0.item_title,
    t0.pfsku_title,
    t0.item_image as first_image,
    t0.pfsku_image,
    t0.category_name,
    t0.shop_name,
    t0.brand_name,
    t0.item_url,
    t0.pfsku_url,
    t0.pfsku_value_sales,
    t0.pfsku_unit_sales,
    t0.pfsku_discount_price,
    t1.tags,
    t0.month_dt
FROM {source_table_1} t0
LEFT JOIN {source_table_2} t1 ON t0.unique_id = t1.unique_id
WHERE (
    t0.pfsku_value_sales > 0
    or t0.shop_id in (
        '1000084032',
        '155843478',
        '279110807',
        '1000084011',
        '1000314981',
        '1000001814'
    )
    or t0.shop_name in (
        '威猛先生家庭清洁京东自营旗舰店',
        '庄臣官方旗舰店',
        '威猛先生旗舰店',
        '雷达京东自营旗舰店',
        '雷达佳儿护母婴京东自营旗舰店',
        '庄臣京东自营旗舰店'
    )
    or (
        (t0.shop_id = '67597230' or t0.shop_name = '天猫超市')
        and t0.manufacturer = 'SCJOHNSN' 
    )
  )
    AND {month_condition}
'''

# 执行基础查询并添加text_hash_id字段
base_df = spark.sql(base_query)
base_df_with_hash = base_df.withColumn("text_hash_id", generate_text_hash_id_udf(base_df.item_title, base_df.pfsku_title))

# 小文件repartition处理：repartition到200个分区进行数据处理，平衡性能和资源使用
base_df_with_hash = base_df_with_hash.repartition(200)
base_df_with_hash.createOrReplaceTempView('base_data_with_hash')

# 计算top80和month_start字段 - 从2022-07开始，基于text_hash_id匹配历史month_start（优化版）
month_start_query = '''
WITH data_with_top80 AS (
  SELECT 
      *,
      SUM(pfsku_value_sales) OVER (
          PARTITION BY platform, month_dt 
          ORDER BY pfsku_value_sales DESC, item_id, pfsku_id
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) / SUM(pfsku_value_sales) OVER (PARTITION BY platform, month_dt) AS top80
  FROM base_data_with_hash
),
-- 使用窗口函数按时间顺序计算month_start
-- 全量模式下，使用窗口函数在同一个查询中处理所有月份
data_with_month_start AS (
  SELECT 
    *,
    -- 使用窗口函数计算month_start
    -- 按platform、item_id、pfsku_id分组，按month_dt排序
    -- 对于每个记录，查找之前月份中相同text_hash_id的首次出现月份
    CASE 
      WHEN ROW_NUMBER() OVER (PARTITION BY platform, item_id, pfsku_id ORDER BY month_dt) = 1 THEN month_dt  -- 首月
      ELSE COALESCE(
        -- 查找之前月份中相同text_hash_id的首次出现月份
        FIRST_VALUE(month_dt) OVER (
          PARTITION BY platform, item_id, pfsku_id, text_hash_id 
          ORDER BY month_dt 
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 
        month_dt
      )
    END as month_start
  FROM data_with_top80
),
month_start_calculation AS (
  SELECT *
  FROM data_with_month_start
)
SELECT 
    platform, item_id, pfsku_id, item_title, pfsku_title, first_image,
    pfsku_image, category_name, shop_name, brand_name, item_url,
    pfsku_url, tags, pfsku_value_sales, pfsku_unit_sales, pfsku_discount_price,
        top80,
        month_start,
        text_hash_id,
        month_dt
FROM month_start_calculation
'''

# 先创建目标表
spark.sql(DDL)

final_df = spark.sql(month_start_query)
# 最终数据repartition到合理分区数，减少小文件数量
# 根据数据量动态调整：增量模式用较少分区，全量模式用较多分区
repartition_num = 2 if update_mode == 'incremental' else 8
final_df = final_df.repartition(repartition_num)
final_df.createOrReplaceTempView('final_data')

# 性能优化配置
spark.sql('SET spark.sql.parquet.compression.codec=gzip')
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
# 优化JOIN性能
spark.sql("set spark.sql.adaptive.enabled=true")
spark.sql("set spark.sql.adaptive.coalescePartitions.enabled=true")
spark.sql("set spark.sql.adaptive.skewJoin.enabled=true")
# 优化广播JOIN
spark.sql("set spark.sql.autoBroadcastJoinThreshold=50MB")

# 插入数据到目标表
spark.sql(f'''
INSERT OVERWRITE TABLE {result_table}
PARTITION ({month_partition})
SELECT 
    platform,
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
    pfsku_value_sales,
    pfsku_unit_sales,
    pfsku_discount_price,
    top80,
    month_start,
    text_hash_id,
    month_dt
FROM final_data
''')

spark.stop()

# Impala统计更新
impala.execute(f'invalidate metadata {result_table}')
impala.execute(drop_incremental_stats)
impala.execute(f'compute incremental stats {result_table}')
