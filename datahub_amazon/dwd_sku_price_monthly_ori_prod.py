# -*- coding: utf-8 -*-
"""
dwd_sku_price_monthly_ori 月度SKU价格表ETL脚本
从 datahub_amazon.dwd_sku_price_daily 生成月度价格统计表
包含价格统计信息：最低价、最高价、平均价、众数等
"""

import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, min as spark_min, max as spark_max, avg as spark_avg, count as spark_count
from pyspark.sql.window import Window
from pigeon.connector import new_impala_connector
from pigeon.utils import init_logging

# ==================== 配置参数 ====================
TARGET_DB = 'datahub_amazon'
TARGET_TABLE = 'dwd_sku_price_monthly_ori'
CALC_PARTITION = '{{ yesterday_ds }}'  
REFRESH_STATS = False
EMR = True
PARTITION_NUM = 200
COMPRESSION = 'snappy'
TEST_LIMIT = None  # 生产环境不使用限制
UPSTREAM_TABLE = 'datahub_amazon.dwd_sku_price_daily'
FEISHU_URL = 'https://yimiandata.feishu.cn/wiki/E81Zw4jK7iJYbGkh5Ejcc3vjnmh?sheet=alzKGw'

# ==================== 表结构DDL ====================
DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_DB}.{TARGET_TABLE} (
    product_id STRING COMMENT '商品ID',
    sku_id STRING COMMENT 'sku id',
    sku_list_price DOUBLE COMMENT 'sku页面价格-默认',
    sku_list_price_currency STRING COMMENT 'sku页面价格货币',
    sku_online_price DOUBLE COMMENT 'sku到手价价格-默认',
    sku_online_price_currency STRING COMMENT 'sku到手价价格货币',
    sku_list_price_min DOUBLE COMMENT 'sku页面价格月度最低',
    sku_list_price_max DOUBLE COMMENT 'sku页面价格月度最高',
    sku_list_price_avg DOUBLE COMMENT 'sku页面价格月度平均',
    sku_list_price_mode DOUBLE COMMENT 'sku页面价格月度众数',
    sku_list_price_midrange DOUBLE COMMENT 'sku页面价(最高+最低)/2',
    sku_online_price_min DOUBLE COMMENT 'sku到手价格月度最低',
    sku_online_price_max DOUBLE COMMENT 'sku到手价格月度最高',
    sku_online_price_avg DOUBLE COMMENT 'sku到手价格月度平均',
    sku_online_price_mode DOUBLE COMMENT 'sku到手价格月度众数',
    sku_online_price_midrange DOUBLE COMMENT 'sku到手价(最高+最低)/2'
) PARTITIONED BY (
    region STRING COMMENT '区域',
    month_dt STRING COMMENT '月份'
) STORED AS PARQUET
"""

# ==================== 飞书文档获取函数 ====================
def get_monthly_price_time_range(calc_partition):
    """从飞书文档获取月度价格时间范围配置"""
    from pigeon.connector.feishu import FeishuBot
    
    bot = FeishuBot()
    token = FEISHU_URL.split('/wiki/')[1].split('?')[0]
    sheet = FEISHU_URL.split('sheet=')[1].split('&')[0]
    df = bot.read_feishusheet(file_token=token, sheet=sheet)
    
    if df.empty or 'start_dt' not in df.columns or 'end_dt' not in df.columns:
        return None, None
    
    # 计算t-1月（昨天所在月份的上一个月）
    calc_date = datetime.strptime(calc_partition, '%Y-%m-%d')
    prev_month = calc_date - relativedelta(months=1)
    target_month = prev_month.strftime('%Y-%m-01')
    
    # 查找匹配的月份配置
    matching_rows = df[df['month_dt'] == target_month]
    
    if not matching_rows.empty:
        return matching_rows['start_dt'].iloc[0], matching_rows['end_dt'].iloc[0]
    return None, None

def get_default_time_range(calc_partition):
    """获取默认时间范围：t-1月第一天到t-1月最后一天"""
    calc_date = datetime.strptime(calc_partition, '%Y-%m-%d')
    prev_month = calc_date - relativedelta(months=1)
    
    start_date = prev_month.replace(day=1).strftime('%Y-%m-%d')
    next_month = prev_month + relativedelta(months=1)
    end_date = (next_month.replace(day=1) - timedelta(days=1)).strftime('%Y-%m-%d')
    
    return start_date, end_date

# ==================== 数据处理函数 ====================
def dumper(spark, calc_partition):
    """处理dwd_sku_price_daily -> dwd_sku_price_monthly_ori 月度价格表"""
    limit_clause = f"LIMIT {TEST_LIMIT}" if TEST_LIMIT else ""
    
    # 获取时间范围配置
    feishu_start_date, feishu_end_date = get_monthly_price_time_range(calc_partition)
    
    if feishu_start_date and feishu_end_date:
        time_filter = f"AND dt BETWEEN '{feishu_start_date}' AND '{feishu_end_date}'"
    else:
        default_start_date, default_end_date = get_default_time_range(calc_partition)
        time_filter = f"AND dt BETWEEN '{default_start_date}' AND '{default_end_date}'"
    
    base_query = f"""
    WITH price_stats AS (
        SELECT 
            sku_id,
            region,
            date_format(dt, 'yyyy-MM-01') as month_dt,
            
            -- 使用窗口函数计算最小价格
            MIN(CASE WHEN sku_list_price IS NOT NULL AND sku_list_price > 0 THEN sku_list_price END) 
                OVER (PARTITION BY sku_id, region, date_format(dt, 'yyyy-MM-01')) as min_list_price,
            MIN(CASE WHEN sku_online_price IS NOT NULL AND sku_online_price > 0 THEN sku_online_price END) 
                OVER (PARTITION BY sku_id, region, date_format(dt, 'yyyy-MM-01')) as min_online_price,
            
            -- 价格统计（使用窗口函数，只计算有效价格）
            MIN(CASE WHEN sku_list_price IS NOT NULL AND sku_list_price > 0 THEN sku_list_price END) 
                OVER (PARTITION BY sku_id, region, date_format(dt, 'yyyy-MM-01')) as sku_list_price_min,
            MAX(CASE WHEN sku_list_price IS NOT NULL AND sku_list_price > 0 THEN sku_list_price END) 
                OVER (PARTITION BY sku_id, region, date_format(dt, 'yyyy-MM-01')) as sku_list_price_max,
            AVG(CASE WHEN sku_list_price IS NOT NULL AND sku_list_price > 0 THEN sku_list_price END) 
                OVER (PARTITION BY sku_id, region, date_format(dt, 'yyyy-MM-01')) as sku_list_price_avg,
            MIN(CASE WHEN sku_online_price IS NOT NULL AND sku_online_price > 0 THEN sku_online_price END) 
                OVER (PARTITION BY sku_id, region, date_format(dt, 'yyyy-MM-01')) as sku_online_price_min,
            MAX(CASE WHEN sku_online_price IS NOT NULL AND sku_online_price > 0 THEN sku_online_price END) 
                OVER (PARTITION BY sku_id, region, date_format(dt, 'yyyy-MM-01')) as sku_online_price_max,
            AVG(CASE WHEN sku_online_price IS NOT NULL AND sku_online_price > 0 THEN sku_online_price END) 
                OVER (PARTITION BY sku_id, region, date_format(dt, 'yyyy-MM-01')) as sku_online_price_avg,
            
            -- 取与最小价格同一条记录的币种和product_id
            FIRST_VALUE(sku_list_price_currency) OVER (
                PARTITION BY sku_id, region, date_format(dt, 'yyyy-MM-01')
                ORDER BY 
                    CASE WHEN sku_list_price = MIN(CASE WHEN sku_list_price IS NOT NULL AND sku_list_price > 0 THEN sku_list_price END) OVER (PARTITION BY sku_id, region, date_format(dt, 'yyyy-MM-01')) THEN 0 ELSE 1 END,
                    CASE WHEN sku_list_price_currency IS NOT NULL AND sku_list_price_currency != '' THEN 0 ELSE 1 END,
                    dt DESC
            ) as sku_list_price_currency,
            
            FIRST_VALUE(sku_online_price_currency) OVER (
                PARTITION BY sku_id, region, date_format(dt, 'yyyy-MM-01')
                ORDER BY 
                    CASE WHEN sku_online_price = MIN(CASE WHEN sku_online_price IS NOT NULL AND sku_online_price > 0 THEN sku_online_price END) OVER (PARTITION BY sku_id, region, date_format(dt, 'yyyy-MM-01')) THEN 0 ELSE 1 END,
                    CASE WHEN sku_online_price_currency IS NOT NULL AND sku_online_price_currency != '' THEN 0 ELSE 1 END,
                    dt DESC
            ) as sku_online_price_currency,
            
            -- product_id：优先取对应最小价格记录的，如果为空则取非空非''的
            FIRST_VALUE(product_id) OVER (
                PARTITION BY sku_id, region, date_format(dt, 'yyyy-MM-01')
                ORDER BY 
                    CASE WHEN sku_list_price = MIN(CASE WHEN sku_list_price IS NOT NULL AND sku_list_price > 0 THEN sku_list_price END) OVER (PARTITION BY sku_id, region, date_format(dt, 'yyyy-MM-01')) 
                         AND product_id IS NOT NULL AND product_id != '' THEN 0 
                         WHEN product_id IS NOT NULL AND product_id != '' THEN 1
                         ELSE 2 END,
                    dt DESC
            ) as product_id,
            
            -- 众数计算
            ROW_NUMBER() OVER (
                PARTITION BY sku_id, region, date_format(dt, 'yyyy-MM-01')
                ORDER BY COUNT(sku_list_price) OVER (PARTITION BY sku_id, region, date_format(dt, 'yyyy-MM-01'), sku_list_price) DESC, sku_list_price ASC
            ) as list_price_mode_rn,
            sku_list_price as list_price_for_mode,
            
            ROW_NUMBER() OVER (
                PARTITION BY sku_id, region, date_format(dt, 'yyyy-MM-01')
                ORDER BY COUNT(sku_online_price) OVER (PARTITION BY sku_id, region, date_format(dt, 'yyyy-MM-01'), sku_online_price) DESC, sku_online_price ASC
            ) as online_price_mode_rn,
            sku_online_price as online_price_for_mode,
            
            ROW_NUMBER() OVER (
                PARTITION BY sku_id, region, date_format(dt, 'yyyy-MM-01')
                ORDER BY dt DESC
            ) as rn
            
        FROM {UPSTREAM_TABLE}
        WHERE 1=1
          {time_filter}
          AND sku_id IS NOT NULL AND sku_id != ''
          AND region IS NOT NULL AND region != ''
          AND (sku_online_price IS NOT NULL OR sku_list_price IS NOT NULL)
        {limit_clause}
    ),
    
    mode_calculated AS (
        SELECT 
            sku_id,
            region,
            month_dt,
            product_id,
            sku_list_price_currency,
            sku_online_price_currency,
            -- 默认价格字段：按文档要求取min值作为默认值
            sku_list_price_min as sku_list_price,
            sku_online_price_min as sku_online_price,
            sku_list_price_min,
            sku_list_price_max,
            sku_list_price_avg,
            sku_online_price_min,
            sku_online_price_max,
            sku_online_price_avg,
            
            -- 众数：取出现次数最多价格，平局时取最低价
            MAX(CASE WHEN list_price_mode_rn = 1 THEN list_price_for_mode END) as sku_list_price_mode,
            MAX(CASE WHEN online_price_mode_rn = 1 THEN online_price_for_mode END) as sku_online_price_mode,
            
            -- 中位数：按文档要求留空
            NULL as sku_list_price_midrange,
            NULL as sku_online_price_midrange
            
        FROM price_stats
        WHERE rn = 1
        GROUP BY sku_id, region, month_dt, product_id, sku_list_price_currency, 
                 sku_online_price_currency, sku_list_price_min, sku_list_price_max, 
                 sku_list_price_avg, sku_online_price_min, sku_online_price_max, 
                 sku_online_price_avg
    )
    
    SELECT 
        product_id,
        sku_id,
        sku_list_price,
        sku_list_price_currency,
        sku_online_price,
        sku_online_price_currency,
        sku_list_price_min,
        sku_list_price_max,
        sku_list_price_avg,
        sku_list_price_mode,
        sku_list_price_midrange,
        sku_online_price_min,
        sku_online_price_max,
        sku_online_price_avg,
        sku_online_price_mode,
        sku_online_price_midrange,
        region,
        month_dt
    FROM mode_calculated
    """
    
    # 执行SQL获取最终结果
    df = spark.sql(base_query)
    
    return df

def loader(spark, df: DataFrame, calc_partition):
    """写入Hive表 - 动态分区覆盖写入"""
    # 确保表存在
    spark.sql(DDL)
    
    # 创建临时视图
    df.createOrReplaceTempView("temp_dwd_sku_price_monthly_ori")
    
    # 使用INSERT OVERWRITE动态分区写入
    overwrite_sql = f"""
    INSERT OVERWRITE TABLE {TARGET_DB}.{TARGET_TABLE}
    PARTITION (region, month_dt)
    SELECT 
        product_id, sku_id, sku_list_price, sku_list_price_currency,
        sku_online_price, sku_online_price_currency,
        sku_list_price_min, sku_list_price_max, sku_list_price_avg,
        sku_list_price_mode, sku_list_price_midrange,
        sku_online_price_min, sku_online_price_max, sku_online_price_avg,
        sku_online_price_mode, sku_online_price_midrange,
        region, month_dt
    FROM temp_dwd_sku_price_monthly_ori
    """
    
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
    impala.execute(f"COMPUTE INCREMENTAL STATS {TARGET_DB}.{TARGET_TABLE}")

if __name__ == '__main__':
    exit(main())

