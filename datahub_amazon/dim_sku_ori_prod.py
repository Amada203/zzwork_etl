# -*- coding: utf-8 -*-
"""
dim_sku_ori 维度表ETL脚本
从 datahub_amazon.dwd_sku_daily 生成SKU维度表
每个字段独立取非空非""最新一条记录
注意：取截止到当天的历史数据，确保性能和时间字段的准确性
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pigeon.connector import new_impala_connector
from pigeon.utils import init_logging

# ==================== 配置参数 ====================
TARGET_DB = 'datahub_amazon'
TARGET_TABLE = 'dim_sku_ori'
CALC_PARTITION = '{{ yesterday_ds }}'  # Jinja2 模板变量，在 Airflow 中会被替换
REFRESH_STATS = False
EMR = True
PARTITION_NUM = 200
COMPRESSION = 'snappy'
TEST_LIMIT = None  # 生产环境不使用限制
UPSTREAM_TABLE = 'datahub_amazon.dwd_sku_daily'

# ==================== 表结构DDL ====================
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
    first_snapshot_dt STRING,
    last_snapshot_time STRING
) PARTITIONED BY (
    region STRING
) STORED AS PARQUET
"""

# ==================== 数据处理函数 ====================
def dumper(spark, calc_partition):
    """处理dwd_sku_daily -> dim_sku_ori 维度表"""
    limit_clause = f"LIMIT {TEST_LIMIT}" if TEST_LIMIT else ""
    
    base_query = """
    WITH field_optimized AS (
        SELECT 
            sku_id,
            region,
            FIRST_VALUE(product_id) OVER (
                PARTITION BY sku_id, region 
                ORDER BY 
                    CASE WHEN product_id IS NOT NULL AND product_id != '' THEN 0 ELSE 1 END,
                    snapshot_time DESC
            ) as product_id,
            
            FIRST_VALUE(product_title) OVER (
                PARTITION BY sku_id, region 
                ORDER BY 
                    CASE WHEN product_title IS NOT NULL AND product_title != '' THEN 0 ELSE 1 END,
                    snapshot_time DESC
            ) as product_title,
            
            FIRST_VALUE(url) OVER (
                PARTITION BY sku_id, region 
                ORDER BY 
                    CASE WHEN url IS NOT NULL AND url != '' THEN 0 ELSE 1 END,
                    snapshot_time DESC
            ) as url,
            
            FIRST_VALUE(color) OVER (
                PARTITION BY sku_id, region 
                ORDER BY 
                    CASE WHEN color IS NOT NULL AND color != '' THEN 0 ELSE 1 END,
                    snapshot_time DESC
            ) as color,
            
            FIRST_VALUE(size) OVER (
                PARTITION BY sku_id, region 
                ORDER BY 
                    CASE WHEN size IS NOT NULL AND size != '' THEN 0 ELSE 1 END,
                    snapshot_time DESC
            ) as size,
            
            FIRST_VALUE(brand) OVER (
                PARTITION BY sku_id, region 
                ORDER BY 
                    CASE WHEN brand IS NOT NULL AND brand != '' THEN 0 ELSE 1 END,
                    snapshot_time DESC
            ) as brand,
            
            FIRST_VALUE(brand_id) OVER (
                PARTITION BY sku_id, region 
                ORDER BY 
                    CASE WHEN brand_id IS NOT NULL THEN 0 ELSE 1 END,
                    snapshot_time DESC
            ) as brand_id,
            
            FIRST_VALUE(std_brand_name) OVER (
                PARTITION BY sku_id, region 
                ORDER BY 
                    CASE WHEN std_brand_name IS NOT NULL AND std_brand_name != '' THEN 0 ELSE 1 END,
                    snapshot_time DESC
            ) as std_brand_name,
            
            FIRST_VALUE(manufacturer) OVER (
                PARTITION BY sku_id, region 
                ORDER BY 
                    CASE WHEN manufacturer IS NOT NULL AND manufacturer != '' THEN 0 ELSE 1 END,
                    snapshot_time DESC
            ) as manufacturer,
            
            FIRST_VALUE(has_sku) OVER (
                PARTITION BY sku_id, region 
                ORDER BY 
                    CASE WHEN has_sku IS NOT NULL THEN 0 ELSE 1 END,
                    snapshot_time DESC
            ) as has_sku,
            
            FIRST_VALUE(variant_information) OVER (
                PARTITION BY sku_id, region 
                ORDER BY 
                    CASE WHEN variant_information IS NOT NULL AND variant_information != '' THEN 0 ELSE 1 END,
                    snapshot_time DESC
            ) as variant_information,
            
            FIRST_VALUE(category) OVER (
                PARTITION BY sku_id, region 
                ORDER BY 
                    CASE WHEN category IS NOT NULL AND category != '' THEN 0 ELSE 1 END,
                    snapshot_time DESC
            ) as category,
            
            -- sub_category取与category同一条记录的值
            FIRST_VALUE(sub_category) OVER (
                PARTITION BY sku_id, region 
                ORDER BY 
                    CASE WHEN category IS NOT NULL AND category != '' THEN 0 ELSE 1 END,
                    snapshot_time DESC
            ) as sub_category,
            
            FIRST_VALUE(seller) OVER (
                PARTITION BY sku_id, region 
                ORDER BY 
                    CASE WHEN seller IS NOT NULL AND seller != '' THEN 0 ELSE 1 END,
                    snapshot_time DESC
            ) as seller,
            
            FIRST_VALUE(seller_id) OVER (
                PARTITION BY sku_id, region 
                ORDER BY 
                    CASE WHEN seller_id IS NOT NULL AND seller_id != '' THEN 0 ELSE 1 END,
                    snapshot_time DESC
            ) as seller_id,
            
            FIRST_VALUE(first_image) OVER (
                PARTITION BY sku_id, region 
                ORDER BY 
                    CASE WHEN first_image IS NOT NULL AND first_image != '' THEN 0 ELSE 1 END,
                    snapshot_time DESC
            ) as first_image,
            
            FIRST_VALUE(imags) OVER (
                PARTITION BY sku_id, region 
                ORDER BY 
                    CASE WHEN imags IS NOT NULL AND imags != '' THEN 0 ELSE 1 END,
                    snapshot_time DESC
            ) as imags,
            
            FIRST_VALUE(video) OVER (
                PARTITION BY sku_id, region 
                ORDER BY 
                    CASE WHEN video IS NOT NULL AND video != '' THEN 0 ELSE 1 END,
                    snapshot_time DESC
            ) as video,
            
            FIRST_VALUE(specifications) OVER (
                PARTITION BY sku_id, region 
                ORDER BY 
                    CASE WHEN specifications IS NOT NULL AND specifications != '' THEN 0 ELSE 1 END,
                    snapshot_time DESC
            ) as specifications,
            
            FIRST_VALUE(additional_description) OVER (
                PARTITION BY sku_id, region 
                ORDER BY 
                    CASE WHEN additional_description IS NOT NULL AND additional_description != '' THEN 0 ELSE 1 END,
                    snapshot_time DESC
            ) as additional_description,
            
            FIRST_VALUE(extra_json) OVER (
                PARTITION BY sku_id, region 
                ORDER BY 
                    CASE WHEN extra_json IS NOT NULL AND extra_json != '' AND extra_json != '{}' THEN 0 ELSE 1 END,
                    snapshot_time DESC
            ) as extra_json,
            
            FIRST_VALUE(etl_source) OVER (
                PARTITION BY sku_id, region 
                ORDER BY 
                    CASE WHEN etl_source IS NOT NULL AND etl_source != '' THEN 0 ELSE 1 END,
                    snapshot_time DESC
            ) as etl_source,
            
            MIN(dt) OVER (PARTITION BY sku_id, region) as first_snapshot_dt,
            MAX(snapshot_time) OVER (PARTITION BY sku_id, region) as last_snapshot_time,
            
            ROW_NUMBER() OVER (
                PARTITION BY sku_id, region 
                ORDER BY snapshot_time DESC
            ) as rn
        FROM UPSTREAM_TABLE_PLACEHOLDER
        WHERE dt <= 'CALC_PARTITION_PLACEHOLDER'
          AND region IS NOT NULL AND region != ''
          AND sku_id IS NOT NULL AND sku_id != ''
        LIMIT_PLACEHOLDER
    ),
    
    deduplicated AS (
        SELECT 
            sku_id,
            region,
            product_id,
            product_title,
            url,
            color,
            size,
            brand,
            brand_id,
            std_brand_name,
            manufacturer,
            has_sku,
            variant_information,
            category,
            sub_category,
            CAST(NULL AS STRING) as category_id,
            -- category_name从sub_category最后一个>后面的内容
            CASE 
                WHEN sub_category IS NOT NULL AND sub_category != '' 
                THEN split(sub_category, '>')[size(split(sub_category, '>')) - 1]
                ELSE NULL 
            END as category_name,
            CAST(NULL AS INT) as category_1_id,
            -- category_1_name使用category字段
            category as category_1_name,
            CAST(NULL AS INT) as category_2_id,
            -- 从sub_category分割获取各层级分类名称
            CASE 
                WHEN sub_category IS NOT NULL AND sub_category != '' AND size(split(sub_category, '>')) >= 1
                THEN split(sub_category, '>')[0]
                ELSE NULL 
            END as category_2_name,
            CAST(NULL AS INT) as category_3_id,
            CASE 
                WHEN sub_category IS NOT NULL AND sub_category != '' AND size(split(sub_category, '>')) >= 2
                THEN split(sub_category, '>')[1]
                ELSE NULL 
            END as category_3_name,
            CAST(NULL AS INT) as category_4_id,
            CASE 
                WHEN sub_category IS NOT NULL AND sub_category != '' AND size(split(sub_category, '>')) >= 3
                THEN split(sub_category, '>')[2]
                ELSE NULL 
            END as category_4_name,
            CAST(NULL AS INT) as category_5_id,
            CASE 
                WHEN sub_category IS NOT NULL AND sub_category != '' AND size(split(sub_category, '>')) >= 4
                THEN split(sub_category, '>')[3]
                ELSE NULL 
            END as category_5_name,
            CAST(NULL AS INT) as category_6_id,
            CASE 
                WHEN sub_category IS NOT NULL AND sub_category != '' AND size(split(sub_category, '>')) >= 5
                THEN split(sub_category, '>')[4]
                ELSE NULL 
            END as category_6_name,
            CAST(NULL AS INT) as category_7_id,
            CASE 
                WHEN sub_category IS NOT NULL AND sub_category != '' AND size(split(sub_category, '>')) >= 6
                THEN split(sub_category, '>')[5]
                ELSE NULL 
            END as category_7_name,
            CAST(NULL AS INT) as category_8_id,
            CASE 
                WHEN sub_category IS NOT NULL AND sub_category != '' AND size(split(sub_category, '>')) >= 7
                THEN split(sub_category, '>')[6]
                ELSE NULL 
            END as category_8_name,
            CAST(NULL AS INT) as category_9_id,
            CASE 
                WHEN sub_category IS NOT NULL AND sub_category != '' AND size(split(sub_category, '>')) >= 8
                THEN split(sub_category, '>')[7]
                ELSE NULL 
            END as category_9_name,  
            seller,
            seller_id,
            first_image,
            imags,
            video,
            specifications,
            additional_description,
            extra_json,
            etl_source,
            first_snapshot_dt,
            last_snapshot_time
        FROM field_optimized
        WHERE rn = 1
    )
    
    SELECT * FROM deduplicated
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
    df.createOrReplaceTempView("temp_dim_sku_ori")
    
    # 使用INSERT OVERWRITE动态分区写入
    overwrite_sql = """
    INSERT OVERWRITE TABLE TARGET_DB_PLACEHOLDER.TARGET_TABLE_PLACEHOLDER
    PARTITION (region)
    SELECT 
        product_id, sku_id, product_title, url, color, size, brand, brand_id, std_brand_name,
        manufacturer, has_sku, variant_information, category, sub_category, category_id, category_name,
        category_1_id, category_1_name, category_2_id, category_2_name, category_3_id, category_3_name,
        category_4_id, category_4_name, category_5_id, category_5_name, category_6_id, category_6_name,
        category_7_id, category_7_name, category_8_id, category_8_name, category_9_id, category_9_name,
        seller, seller_id, first_image, imags, video, specifications, additional_description, extra_json,
        etl_source, first_snapshot_dt, last_snapshot_time, region
    FROM temp_dim_sku_ori
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
    spark = SparkSession.builder.appName("oneflow.datahub_amazon.dim_sku_ori").enableHiveSupport().getOrCreate()
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
