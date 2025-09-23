#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
dwd_sku_daily 日度SKU表ETL脚本
从 datahub_amazon.dwd_sku_info 生成日度汇总表
每个字段分别取当天非空非null，etl_source优先，snapshot time最晚的值
"""

import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, collect_list, when, col
from pyspark.sql.types import StringType, ArrayType
from pigeon.connector import new_impala_connector
from pigeon.utils import init_logging
from dw_util.util import str_utils
# from spark_util.insert import spark_write_hive  # 未使用

# ==================== 配置参数 ====================
TARGET_DB = 'datahub_amazon'
TARGET_TABLE = 'dwd_sku_daily'
CALC_PARTITION = '{{ yesterday_ds }}'
REFRESH_STATS = False
EMR = True
PARTITION_NUM = 400
COMPRESSION = 'snappy'
TEST_LIMIT = 1000  # None
UPSTREAM_TABLE = 'datahub_amazon.dwd_sku_info'
FEISHU_URL = 'https://yimiandata.feishu.cn/wiki/E81Zw4jK7iJYbGkh5Ejcc3vjnmh?sheet=h2Kd9N'


DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_DB}.{TARGET_TABLE} (
    product_id STRING,
    sku_id STRING,
    product_title STRING,
    url STRING,
    color STRING,
    size STRING,
    brand STRING,
    brand_id BIGINT,
    std_brand_name STRING,
    manufacturer STRING,
    has_sku INT,
    variant_information STRING,
    category STRING,
    sub_category STRING,
    category_id STRING,
    category_name STRING,
    category_1_id INT,
    category_1_name STRING,
    category_2_id INT,
    category_2_name STRING,
    category_3_id INT,
    category_3_name STRING,
    category_4_id INT,
    category_4_name STRING,
    category_5_id INT,
    category_5_name STRING,
    category_6_id INT,
    category_6_name STRING,
    category_7_id INT,
    category_7_name STRING,
    category_8_id INT,
    category_8_name STRING,
    category_9_id INT,
    category_9_name STRING,
    seller STRING,
    seller_id STRING,
    first_image STRING,
    imags STRING,
    video STRING,
    specifications STRING,
    additional_description STRING,
    extra_json STRING,
    etl_source STRING,
    snapshot_time STRING
) PARTITIONED BY (
    region STRING,
    dt STRING
) STORED AS PARQUET
"""

# QUERY_COLUMNS = str_utils.get_query_column_from_ddl(DDL)  # 未使用
# PARTITION_DICT = None  # 未使用

# ==================== 飞书文档获取函数 ====================
def get_etl_source_priority():
    """从飞书文档获取etl_source优先级排序"""
    from pigeon.connector.feishu import FeishuBot
    
    bot = FeishuBot()
    token = FEISHU_URL.split('/wiki/')[1].split('?')[0]
    sheet = FEISHU_URL.split('sheet=')[1].split('&')[0]
    df = bot.read_feishusheet(file_token=token, sheet=sheet)
    
    etl_source_priority = df['etl_source'].tolist()
    logging.info(f"获取到etl_source优先级: {len(etl_source_priority)}个")
    return etl_source_priority

def build_priority_case_statement(etl_source_priority):
    """构建etl_source优先级排序的CASE语句"""
    case_parts = [f"WHEN '{source}' THEN {i}" for i, source in enumerate(etl_source_priority, 1)]
    return f"CASE etl_source {' '.join(case_parts)} ELSE {len(etl_source_priority) + 1} END"

# ==================== 工具函数 ====================
def execute_sql(sql, executor):
    """执行SQL查询"""
    return executor(sql)

def merge_json_udf(json_list):
    """
    高效的JSON合并UDF
    相同key取最优，不同key合并
    """
    if not json_list or len(json_list) == 0:
        return '{}'
    
    merged_dict = {}
    
    for json_str in json_list:
        if not json_str or json_str.strip() == '' or json_str.strip() == '{}':
            continue
            
        try:
            data = json.loads(json_str)
            if isinstance(data, dict):
                # 合并字典：相同key取最优（后面的覆盖前面的）
                merged_dict.update(data)
        except (json.JSONDecodeError, TypeError):
            # 忽略无效的JSON
            continue
    
    return json.dumps(merged_dict, ensure_ascii=False)

# 注册UDF
merge_json_udf_func = udf(merge_json_udf, StringType())

# ==================== 数据处理函数 ====================
# 移除动态SQL生成函数，直接使用静态SQL

def dumper_daily_sku(spark, calc_partition):
    """处理dwd_sku_info -> dwd_sku_daily 日度表"""
    limit_clause = f"LIMIT {TEST_LIMIT}" if TEST_LIMIT else ""
    
    # 获取etl_source优先级排序
    etl_source_priority = get_etl_source_priority()
    priority_case_statement = build_priority_case_statement(etl_source_priority)
    
    # 定义变量避免f-string语法错误
    empty_json = '{}'
    comma_sep = ','
    
    base_query = f"""
    WITH source_data AS (
        SELECT *,
        FIRST_VALUE(if(sku_id IS NULL OR sku_id = '', NULL, sku_id)) IGNORE NULLS OVER (
                PARTITION BY  region, dt 
                ORDER BY {priority_case_statement}, snapshot_time DESC
            ) as optimized_sku_id
        FROM {UPSTREAM_TABLE}
        WHERE dt = '{calc_partition}'
          AND region IS NOT NULL AND region != ''
        {limit_clause}
    ),
    
    -- 收集所有非空且不重复的extra_json
    extra_json_collected AS (
        SELECT 
            optimized_sku_id,
            region,
            dt,
            COLLECT_SET(
                CASE 
                    WHEN extra_json IS NOT NULL AND extra_json NOT IN ('', '{empty_json}')
                    THEN extra_json
                    ELSE NULL
                END
            ) as extra_json_list
        FROM source_data
        GROUP BY optimized_sku_id, region, dt
    ),
    
    
    
    
    -- 对每个字段分别取最优值：当天非空非null，etl_source优先，snapshot time最晚
    field_optimized AS (
        SELECT 
            optimized_sku_id as sku_id,
            region,
            dt,
            
            FIRST_VALUE(if(product_id IS NULL OR product_id = '', NULL, product_id)) IGNORE NULLS OVER (
                PARTITION BY optimized_sku_id, region, dt 
                ORDER BY {priority_case_statement}, snapshot_time DESC
            ) as product_id,
            
            FIRST_VALUE(if(product_title IS NULL OR product_title = '', NULL, product_title)) IGNORE NULLS OVER (
                PARTITION BY optimized_sku_id, region, dt 
                ORDER BY {priority_case_statement}, snapshot_time DESC
            ) as product_title,
            
            FIRST_VALUE(if(url IS NULL OR url = '', NULL, url)) IGNORE NULLS OVER (
                PARTITION BY optimized_sku_id, region, dt 
                ORDER BY {priority_case_statement}, snapshot_time DESC
            ) as url,
            
            FIRST_VALUE(if(color IS NULL OR color = '', NULL, color)) IGNORE NULLS OVER (
                PARTITION BY optimized_sku_id, region, dt 
                ORDER BY {priority_case_statement}, snapshot_time DESC
            ) as color,
            
            FIRST_VALUE(if(size IS NULL OR size = '', NULL, size)) IGNORE NULLS OVER (
                PARTITION BY optimized_sku_id, region, dt 
                ORDER BY {priority_case_statement}, snapshot_time DESC
            ) as size,
            
            FIRST_VALUE(if(brand IS NULL OR brand = '', NULL, brand)) IGNORE NULLS OVER (
                PARTITION BY optimized_sku_id, region, dt 
                ORDER BY {priority_case_statement}, snapshot_time DESC
            ) as brand,
            
            FIRST_VALUE(if(manufacturer IS NULL OR manufacturer = '', NULL, manufacturer)) IGNORE NULLS OVER (
                PARTITION BY optimized_sku_id, region, dt 
                ORDER BY {priority_case_statement}, snapshot_time DESC
            ) as manufacturer,
            
            FIRST_VALUE(if(has_sku IS NULL, NULL, has_sku)) IGNORE NULLS OVER (
                PARTITION BY optimized_sku_id, region, dt 
                ORDER BY {priority_case_statement}, snapshot_time DESC
            ) as has_sku,
            
            FIRST_VALUE(if(variant_information IS NULL OR variant_information = '', NULL, variant_information)) IGNORE NULLS OVER (
                PARTITION BY optimized_sku_id, region, dt 
                ORDER BY {priority_case_statement}, snapshot_time DESC
            ) as variant_information,
            
            FIRST_VALUE(if(category IS NULL OR category = '', NULL, category)) IGNORE NULLS OVER (
                PARTITION BY optimized_sku_id, region, dt 
                ORDER BY {priority_case_statement}, snapshot_time DESC
            ) as category,
            
            FIRST_VALUE(if(sub_category IS NULL OR sub_category = '', NULL, sub_category)) IGNORE NULLS OVER (
                PARTITION BY optimized_sku_id, region, dt 
                ORDER BY {priority_case_statement}, snapshot_time DESC
            ) as sub_category,
            
            FIRST_VALUE(if(seller IS NULL OR seller = '', NULL, seller)) IGNORE NULLS OVER (
                PARTITION BY optimized_sku_id, region, dt 
                ORDER BY {priority_case_statement}, snapshot_time DESC
            ) as seller,
            
            FIRST_VALUE(if(seller_id IS NULL OR seller_id = '', NULL, seller_id)) IGNORE NULLS OVER (
                PARTITION BY optimized_sku_id, region, dt 
                ORDER BY {priority_case_statement}, snapshot_time DESC
            ) as seller_id,
            
            FIRST_VALUE(if(first_image IS NULL OR first_image = '', NULL, first_image)) IGNORE NULLS OVER (
                PARTITION BY optimized_sku_id, region, dt 
                ORDER BY {priority_case_statement}, snapshot_time DESC
            ) as first_image,
            
            FIRST_VALUE(if(imags IS NULL OR imags = '', NULL, imags)) IGNORE NULLS OVER (
                PARTITION BY optimized_sku_id, region, dt 
                ORDER BY {priority_case_statement}, snapshot_time DESC
            ) as imags,
            
            FIRST_VALUE(if(video IS NULL OR video = '', NULL, video)) IGNORE NULLS OVER (
                PARTITION BY optimized_sku_id, region, dt 
                ORDER BY {priority_case_statement}, snapshot_time DESC
            ) as video,
            
            FIRST_VALUE(if(specifications IS NULL OR specifications = '', NULL, specifications)) IGNORE NULLS OVER (
                PARTITION BY optimized_sku_id, region, dt 
                ORDER BY {priority_case_statement}, snapshot_time DESC
            ) as specifications,
            
            FIRST_VALUE(if(additional_description IS NULL OR additional_description = '', NULL, additional_description)) IGNORE NULLS OVER (
                PARTITION BY optimized_sku_id, region, dt 
                ORDER BY {priority_case_statement}, snapshot_time DESC
            ) as additional_description,
            
            -- extra_json 处理：取优先级最高的非空值
            FIRST_VALUE(if(extra_json IS NOT NULL AND extra_json != '' AND extra_json != '{empty_json}', extra_json, NULL)) IGNORE NULLS OVER (
                PARTITION BY optimized_sku_id, region, dt 
                ORDER BY {priority_case_statement}, snapshot_time DESC
            ) as extra_json,
            
            CAST(NULL AS STRING) as etl_source,
            
            FIRST_VALUE(snapshot_time) OVER (
                PARTITION BY optimized_sku_id, region, dt 
                ORDER BY {priority_case_statement}, snapshot_time DESC
            ) as snapshot_time,
            
            -- 添加行号用于去重
            ROW_NUMBER() OVER (
                PARTITION BY optimized_sku_id, region, dt 
                ORDER BY {priority_case_statement}, snapshot_time DESC
            ) as rn
        FROM source_data
    ),
    
    -- 去重：只保留每个分区的第一条记录
    deduplicated AS (
        SELECT 
            sku_id, region, dt, product_id, product_title, url, color, size, brand,
            manufacturer, has_sku, variant_information, category, sub_category,
            seller, seller_id, first_image, imags, video, specifications, 
            additional_description, extra_json, etl_source, snapshot_time
        FROM field_optimized
        WHERE rn = 1
    ),
    
    -- 最终结果：去重并添加预留字段，集成去重后的extra_json
    final_result AS (
        SELECT 
            d.product_id, d.sku_id, d.product_title, d.url, d.color, d.size, d.brand, 
            CAST(NULL AS BIGINT) as brand_id,
            CAST(NULL AS STRING) as std_brand_name,
            d.manufacturer, d.has_sku, d.variant_information,
            d.category, d.sub_category,
            CAST(NULL AS STRING) as category_id,
            CAST(NULL AS STRING) as category_name,
            CAST(NULL AS INT) as category_1_id, CAST(NULL AS STRING) as category_1_name,
            CAST(NULL AS INT) as category_2_id, CAST(NULL AS STRING) as category_2_name,
            CAST(NULL AS INT) as category_3_id, CAST(NULL AS STRING) as category_3_name,
            CAST(NULL AS INT) as category_4_id, CAST(NULL AS STRING) as category_4_name,
            CAST(NULL AS INT) as category_5_id, CAST(NULL AS STRING) as category_5_name,
            CAST(NULL AS INT) as category_6_id, CAST(NULL AS STRING) as category_6_name,
            CAST(NULL AS INT) as category_7_id, CAST(NULL AS STRING) as category_7_name,
            CAST(NULL AS INT) as category_8_id, CAST(NULL AS STRING) as category_8_name,
            CAST(NULL AS INT) as category_9_id, CAST(NULL AS STRING) as category_9_name,
            d.seller, d.seller_id, d.first_image, d.imags, d.video,
            d.specifications, d.additional_description, 
            COALESCE(ejc.extra_json_list, ARRAY()) as extra_json,
            d.etl_source, d.snapshot_time, d.region, d.dt
        FROM deduplicated d
        LEFT JOIN extra_json_collected ejc ON d.sku_id = ejc.optimized_sku_id 
            AND d.region = ejc.region AND d.dt = ejc.dt
    )
     
    SELECT * FROM final_result
    """
    
    # 执行SQL获取基础数据
    df = execute_sql(base_query, spark.sql)
    
    # 使用UDF处理extra_json合并（仅在需要时使用）
    if TEST_LIMIT:
        # 测试模式：直接返回
        return df
    
    # 生产模式：处理extra_json合并
    df.createOrReplaceTempView("base_result")
    
    # 获取需要合并extra_json的数据
    merge_query = f"""
    SELECT 
        optimized_sku_id,
        region,
        dt,
        COLLECT_LIST(extra_json) as extra_json_list
    FROM (
        SELECT 
            FIRST_VALUE(if(sku_id IS NULL OR sku_id = '', NULL, sku_id)) IGNORE NULLS OVER (
                PARTITION BY region, dt 
                ORDER BY {priority_case_statement}, snapshot_time DESC
            ) as optimized_sku_id,
            region,
            dt,
            extra_json,
            {priority_case_statement} as priority_rank,
            snapshot_time
        FROM {UPSTREAM_TABLE}
        WHERE dt = '{calc_partition}'
          AND region IS NOT NULL AND region != ''
          AND extra_json IS NOT NULL AND extra_json != '' AND extra_json != '{empty_json}'
    ) ranked_data
    GROUP BY optimized_sku_id, region, dt
    """
    
    merge_df = execute_sql(merge_query, spark.sql)
    
    # 使用UDF合并JSON
    merged_json_df = merge_df.withColumn(
        "merged_extra_json", 
        merge_json_udf_func("extra_json_list")
    ).select("optimized_sku_id", "region", "dt", "merged_extra_json")
    
    # 合并结果
    final_df = df.join(
        merged_json_df, 
        (df.sku_id == merged_json_df.optimized_sku_id) & 
        (df.region == merged_json_df.region) & 
        (df.dt == merged_json_df.dt), 
        "left"
    ).select(
        df["*"], 
        merged_json_df["merged_extra_json"]
    ).withColumn(
        "extra_json", 
        when(col("merged_extra_json").isNotNull(), col("merged_extra_json"))
        .otherwise(col("extra_json").cast("string"))
    ).drop("merged_extra_json")
    
    return final_df

def write_to_hive(spark, df):
    """写入Hive表 - 动态分区覆盖写入"""
    # 确保表存在
    spark.sql(DDL)
    
    # 创建临时视图
    df.createOrReplaceTempView("temp_dwd_sku_daily")
    
    # 使用INSERT OVERWRITE动态分区写入
    overwrite_sql = f"""
    INSERT OVERWRITE TABLE {TARGET_DB}.{TARGET_TABLE}
    PARTITION (region, dt)
    SELECT 
        product_id, sku_id, product_title, url, color, size, brand, brand_id,
        std_brand_name, manufacturer, has_sku, variant_information,
        category, sub_category, category_id, category_name,
        category_1_id, category_1_name, category_2_id, category_2_name,
        category_3_id, category_3_name, category_4_id, category_4_name,
        category_5_id, category_5_name, category_6_id, category_6_name,
        category_7_id, category_7_name, category_8_id, category_8_name,
        category_9_id, category_9_name,
        seller, seller_id, first_image, imags, video,
        specifications, additional_description, extra_json,
        etl_source, snapshot_time,
        region, dt
    FROM temp_dwd_sku_daily
    """
    
    # 执行覆盖写入
    spark.sql(overwrite_sql)

# ==================== 主函数 ====================
def main():
    """主执行函数"""
    init_logging()
    impala = new_impala_connector()
    
    # 创建Spark会话
    spark = SparkSession.builder.appName("oneflow.{{dag_name}}.{{job_name}}").enableHiveSupport().getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    
    # 性能优化配置
    spark.sql(f'SET spark.sql.shuffle.partitions={PARTITION_NUM}')
    spark.sql('SET spark.sql.adaptive.enabled=true')
    spark.sql('SET spark.sql.adaptive.coalescePartitions=true')
    
    # 数据提取和转换
    df = dumper_daily_sku(spark, CALC_PARTITION)
    
    # 数据加载
    write_to_hive(spark, df)
    
    # 关闭Spark会话
    spark.stop()
    impala.execute(f"invalidate metadata {TARGET_DB}.{TARGET_TABLE}")
    impala.execute(f"DROP INCREMENTAL STATS {TARGET_DB}.{TARGET_TABLE} PARTITION(dt='{CALC_PARTITION}')")
    impala.execute(f"COMPUTE INCREMENTAL STATS {TARGET_DB}.{TARGET_TABLE}")

if __name__ == "__main__":
    exit(main())
