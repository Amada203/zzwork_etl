# -*- coding: utf-8 -*-
"""
dwd_sku_price_daily 日度SKU价格表ETL脚本
从 datahub_amazon.dwd_sku_info 生成日度价格汇总表
价格字段取最小值，币种字段取最晚记录
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pigeon.connector import new_impala_connector
from pigeon.utils import init_logging

# ==================== 配置参数 ====================
TARGET_DB = 'datahub_amazon'
TARGET_TABLE = 'dwd_sku_price_daily'
CALC_PARTITION = '{{ yesterday_ds }}'  # Jinja2 模板变量，在 Airflow 中会被替换
REFRESH_STATS = False
EMR = True
PARTITION_NUM = 400
COMPRESSION = 'snappy'
TEST_LIMIT = 1000  # 生产环境不使用限制 
UPSTREAM_TABLE = 'datahub_amazon.dwd_sku_info'

DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_DB}.{TARGET_TABLE} (
    product_id STRING,
    sku_id STRING,
    sku_online_price DOUBLE,
    sku_online_price_currency STRING,
    sku_list_price DOUBLE,
    sku_list_price_currency STRING,
    price_info STRING
) PARTITIONED BY (
    region STRING,
    dt STRING
) STORED AS PARQUET
"""
# ==================== 数据处理函数 ====================
def dumper(spark, calc_partition):
    """处理dwd_sku_info -> dwd_sku_price_daily 日度价格表"""
    limit_clause = f"LIMIT {TEST_LIMIT}" if TEST_LIMIT else ""
    
    base_query = """
    WITH min_prices AS (
        SELECT 
            sku_id,
            region,
            dt,
            MIN(CASE 
                WHEN CAST(sku_online_price AS DOUBLE) > 0 
                THEN CAST(sku_online_price AS DOUBLE) 
                ELSE NULL 
            END) as min_online_price,
            MIN(CASE 
                WHEN CAST(sku_list_price AS DOUBLE) > 0 
                THEN CAST(sku_list_price AS DOUBLE) 
                ELSE NULL 
            END) as min_list_price
        FROM UPSTREAM_TABLE_PLACEHOLDER
        WHERE dt = 'CALC_PARTITION_PLACEHOLDER'
          AND region IS NOT NULL AND region != ''
          AND sku_id IS NOT NULL AND sku_id != ''
          AND (sku_online_price IS NOT NULL OR sku_list_price IS NOT NULL)
        GROUP BY sku_id, region, dt
        LIMIT_PLACEHOLDER
    ),
    
    price_optimized AS (
        SELECT 
            product_id,
            sku_id,
            region,
            dt,
            sku_online_price,
            sku_list_price,
            sku_online_price_currency,
            sku_list_price_currency,
            online_price_source,
            list_price_source
        FROM (
            SELECT 
                s.product_id,
                m.sku_id,
                m.region,
                m.dt,
                m.min_online_price as sku_online_price,
                m.min_list_price as sku_list_price,
                
                -- 取与最小在线价格同一条记录的币种
                FIRST_VALUE(s.sku_online_price_currency) OVER (
                    PARTITION BY m.sku_id, m.region, m.dt 
                    ORDER BY 
                        CASE WHEN CAST(s.sku_online_price AS DOUBLE) = m.min_online_price THEN 0 ELSE 1 END,
                        CASE WHEN s.sku_online_price_currency IS NOT NULL AND s.sku_online_price_currency != '' THEN 0 ELSE 1 END,
                        s.snapshot_time DESC
                ) as sku_online_price_currency,
                
                -- 取与最小标价同一条记录的币种
                FIRST_VALUE(s.sku_list_price_currency) OVER (
                    PARTITION BY m.sku_id, m.region, m.dt 
                    ORDER BY 
                        CASE WHEN CAST(s.sku_list_price AS DOUBLE) = m.min_list_price THEN 0 ELSE 1 END,
                        CASE WHEN s.sku_list_price_currency IS NOT NULL AND s.sku_list_price_currency != '' THEN 0 ELSE 1 END,
                        s.snapshot_time DESC
                ) as sku_list_price_currency,
                
                -- 记录价格字段对应的etl_source
                FIRST_VALUE(s.etl_source) OVER (
                    PARTITION BY m.sku_id, m.region, m.dt 
                    ORDER BY 
                        CASE WHEN CAST(s.sku_online_price AS DOUBLE) = m.min_online_price THEN 0 ELSE 1 END,
                        s.snapshot_time DESC
                ) as online_price_source,
                
                FIRST_VALUE(s.etl_source) OVER (
                    PARTITION BY m.sku_id, m.region, m.dt 
                    ORDER BY 
                        CASE WHEN CAST(s.sku_list_price AS DOUBLE) = m.min_list_price THEN 0 ELSE 1 END,
                        s.snapshot_time DESC
                ) as list_price_source,
                
                -- 添加行号用于去重
                ROW_NUMBER() OVER (
                    PARTITION BY m.sku_id, m.region, m.dt 
                    ORDER BY 
                        CASE WHEN CAST(s.sku_online_price AS DOUBLE) = m.min_online_price THEN 0 ELSE 1 END,
                        s.snapshot_time DESC
                ) as rn
            FROM min_prices m
            LEFT JOIN UPSTREAM_TABLE_PLACEHOLDER s ON (
                m.sku_id = s.sku_id AND 
                m.region = s.region AND 
                m.dt = s.dt
            )
        ) ranked
        WHERE rn = 1
    ),
    final_result AS (
        SELECT 
            product_id,
            sku_id,
            sku_online_price,
            sku_online_price_currency,
            sku_list_price,
            sku_list_price_currency,
            CONCAT('{"sku_online_price":"', COALESCE(online_price_source, ''), '","sku_list_price":"', COALESCE(list_price_source, ''), '"}') as price_info,
            region,
            dt
        FROM price_optimized
    )
     
    SELECT * FROM final_result
    """
    
    # 手动替换SQL中的变量
    query = base_query.replace('CALC_PARTITION_PLACEHOLDER', calc_partition)
    query = query.replace('LIMIT_PLACEHOLDER', limit_clause)
    query = query.replace('UPSTREAM_TABLE_PLACEHOLDER', UPSTREAM_TABLE)
    
    # 执行SQL获取最终结果
    df = spark.sql(query)
    
    return df

def loader(spark, df: DataFrame, calc_partition):
    """写入Hive表 - 动态分区覆盖写入"""
    # 确保表存在
    spark.sql(DDL)
    
    # 创建临时视图
    df.createOrReplaceTempView("temp_dwd_sku_price_daily")
    
    # 使用INSERT OVERWRITE动态分区写入
    overwrite_sql = """
    INSERT OVERWRITE TABLE TARGET_DB_PLACEHOLDER.TARGET_TABLE_PLACEHOLDER
    PARTITION (region, dt)
    SELECT 
        product_id, sku_id, sku_online_price, sku_online_price_currency,
        sku_list_price, sku_list_price_currency, price_info,
        region, dt
    FROM temp_dwd_sku_price_daily
    """
    
    # 替换占位符
    overwrite_sql = overwrite_sql.replace('TARGET_DB_PLACEHOLDER', TARGET_DB)
    overwrite_sql = overwrite_sql.replace('TARGET_TABLE_PLACEHOLDER', TARGET_TABLE)
    
    # 执行覆盖写入
    spark.sql(overwrite_sql)

# ==================== 主函数 ====================
def main():
    """主执行函数"""
    init_logging()
    impala = new_impala_connector()
    
    # 创建连接
    spark = SparkSession.builder.appName("oneflow.{{dag_name}}.{{job_name}}").enableHiveSupport().getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    
    # 性能优化配置
    spark.sql(f'SET spark.sql.shuffle.partitions={PARTITION_NUM}')
    spark.sql('SET spark.sql.adaptive.coalescePartitions=true')
    
    # 准备计算参数
    calc_partition = CALC_PARTITION
    
    # 数据提取和转换
    df = dumper(spark, calc_partition)
    
    # 数据加载
    loader(spark, df, calc_partition)
    
    # 关闭Spark会话
    spark.stop()
    impala.execute(f"invalidate metadata {TARGET_DB}.{TARGET_TABLE}")
    impala.execute(f"DROP INCREMENTAL STATS {TARGET_DB}.{TARGET_TABLE} PARTITION(dt='{CALC_PARTITION}')")
    impala.execute(f"COMPUTE INCREMENTAL STATS {TARGET_DB}.{TARGET_TABLE}")

if __name__ == '__main__':
    exit(main())
