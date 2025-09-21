# -*- coding: utf-8 -*-
import logging
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pigeon.utils import init_logging
from pigeon.connector import new_impala_connector
from dw_util.util import str_utils
from spark_util.insert import spark_write_hive

# ==================== 配置参数 ====================
# 目标数据库和表名
TARGET_DB = 'datahub_amazon'
TARGET_TABLE = 'dwd_sku_info'
# 查询的分区
CALC_PARTITION = '{{ yesterday_ds }}'
# 是否在spark_write_hive中刷新元数据
REFRESH_STATS = False
# 是否为EMR环境
EMR = True
# 分区数
PARTITION_NUM = 400
# 压缩格式
COMPRESSION = 'snappy'

# 测试限制（可选）
TEST_LIMIT = None  # 生产环境建议设为None

# 表处理控制（可选）
PROCESS_TABLES = ['menu_v1', 'menu_detail_v1']  

# 上游表配置 - 每个表有不同的字段映射逻辑
UPSTREAM_TABLES = [
    {
        'table_name': 'datahub_amazon.ods_menu_v1',
        'etl_source': 'datahub_amazon.ods_menu_v1',
        'table_type': 'menu_v1'  # 标识表类型以便使用不同的处理逻辑
    },
    {
        'table_name': 'datahub_amazon.ods_menu_detail_v1', 
        'etl_source': 'datahub_amazon.ods_menu_detail_v1',
        'table_type': 'menu_detail_v1' 
    }
]

# ==================== 表结构DDL ====================
DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_DB}.{TARGET_TABLE} (
    product_id STRING,
    sku_id STRING,
    product_title STRING,
    url STRING,
    sku_online_price STRING,
    sku_online_price_currency STRING,
    sku_list_price STRING,
    sku_list_price_currency STRING,
    promo STRING,
    sell_count_str STRING,
    sell_count BIGINT,
    vague_sell_count STRING,
    sell_count_type STRING,
    color STRING,
    size STRING,
    brand STRING,
    manufacturer STRING,
    has_sku INT,
    variant_information STRING,
    category STRING,
    sub_category STRING,
    category_ids STRING,
    seller STRING,
    seller_id STRING,
    review_rating STRING,
    number_of_reviews BIGINT,
    first_image STRING,
    imags STRING,
    video STRING,
    availability STRING,
    inventory STRING,
    comment_count_str STRING,
    comment_count BIGINT,
    vague_comment_count BIGINT,
    specifications STRING,
    additional_description STRING,
    batch_id STRING,
    task_id STRING,
    project STRING,
    crawl_status STRING,
    extra_json STRING,
    snapshot_time STRING
) PARTITIONED BY (
    region STRING,
    dt STRING,
    etl_source STRING
) STORED AS PARQUET
"""

# 查询的列
QUERY_COLUMNS = str_utils.get_query_column_from_ddl(DDL)
# 由于表有动态分区，不使用get_partition_dict
PARTITION_DICT = None

# ==================== 数据提取和转换 ====================
def dumper_menu_v1(spark, calc_partition, table_config):
    limit_clause = f"LIMIT {TEST_LIMIT}" if TEST_LIMIT else ""
    table_name = table_config['table_name']
    etl_source = table_config['etl_source']
    
    base_query = """
    WITH base_data AS (
        -- 步骤1: ods_menu_v1 基础数据提取
        SELECT 
            CAST(NULL AS STRING) as product_id,
            retailer_product_code as sku_id,
            product_description as product_title,
            url,
            category,
            sub_category,
            batch_id,
            CAST(NULL AS STRING) as task_id,
            CAST(NULL AS STRING) as project,  
            crawl_status,
            snapshot_time,
            dt,
            'ETL_SOURCE_PLACEHOLDER' as etl_source,
            sr
        FROM TABLE_NAME_PLACEHOLDER
        WHERE dt = 'CALC_PARTITION_PLACEHOLDER'
          AND udfs.f_get_json_object(replace(sr,'|',''),'$.ulanziregion') IS NOT NULL 
          AND udfs.f_get_json_object(replace(sr,'|',''),'$.ulanziregion') != ''
        LIMIT_PLACEHOLDER
    ),
    
    json_extracted AS (
        -- 步骤2: ods_menu_v1 JSON字段提取
        SELECT 
            *,
            -- 价格相关字段提取（使用管道符格式）
            get_json_object(sr, '$.ulanzi|online_price') as sku_online_price_raw,
            get_json_object(sr, '$.ulanzi|list_price') as sku_list_price_raw,
            get_json_object(sr, '$.ulanzi|sales_count') as sell_count_str_raw,
            get_json_object(sr, '$.ulanzi|review_rating') as review_rating_raw,
            get_json_object(sr, '$.ulanzi|number_of_reviews') as number_of_reviews_raw,
            get_json_object(sr, '$.ulanzi|region') as region_raw,
            get_json_object(sr, '$.ulanzi|expected_delivery_time') as expected_delivery_time,
            get_json_object(sr, '$.ulanzi|delivery_destination') as delivery_destination,
            CAST(NULL AS STRING) as seller_raw,
            CAST(NULL AS STRING) as seller_id_raw
        FROM base_data
    ),
    
    sales_count_parsed AS (
        -- 步骤3: 销量数据解析 (用SQL替换UDF逻辑，性能提升显著)
        SELECT 
            *,
            CASE 
                -- 解析带单位的销量数据 (如: "1.5k" -> 1500, "2w" -> 20000)
                WHEN lower(sell_count_str_raw) RLIKE '\\\\d+\\\\.?\\\\d*k\\\\s*$'
                THEN cast(regexp_extract(lower(sell_count_str_raw), '(\\\\d+\\\\.?\\\\d*)', 1) as double) * 1000
                WHEN lower(sell_count_str_raw) RLIKE '\\\\d+\\\\.?\\\\d*w\\\\s*$'  
                THEN cast(regexp_extract(lower(sell_count_str_raw), '(\\\\d+\\\\.?\\\\d*)', 1) as double) * 10000
                WHEN sell_count_str_raw RLIKE '^\\\\d+\\\\.?\\\\d*$'
                THEN cast(sell_count_str_raw as double)
                ELSE NULL
            END as vague_sell_count_parsed
        FROM json_extracted
    ),
    
    final_transformed AS (
        -- 步骤4: 最终字段映射和转换
        SELECT 
            -- 基础字段
            product_id,
            sku_id,
            product_title,
            url,
            
            -- 价格字段 (清理和标准化)
            CASE 
                WHEN sku_online_price_raw = '' OR sku_online_price_raw = '0' THEN NULL
                ELSE sku_online_price_raw 
            END as sku_online_price,
            CAST(NULL AS STRING) as sku_online_price_currency,
            
            CASE 
                WHEN sku_list_price_raw = '' OR sku_list_price_raw = '0' THEN NULL
                ELSE sku_list_price_raw 
            END as sku_list_price,
            CAST(NULL AS STRING) as sku_list_price_currency,
            
            -- 促销和销量字段
            CAST(NULL AS STRING) as promo,
            CASE 
                WHEN sell_count_str_raw = '' THEN NULL
                ELSE sell_count_str_raw 
            END as sell_count_str,
            CAST(NULL AS BIGINT) as sell_count,
            
            -- 转换后的销量数据
            CASE 
                WHEN vague_sell_count_parsed IS NOT NULL 
                THEN cast(vague_sell_count_parsed as string)
                ELSE NULL
            END as vague_sell_count,
            
            CASE 
                WHEN vague_sell_count_parsed IS NOT NULL 
                THEN 'last_30_count'
                ELSE NULL
            END as sell_count_type,
            
            -- 商品属性 (暂时为空，可根据需要扩展)
            CAST(NULL AS STRING) as color,
            CAST(NULL AS STRING) as size,
            CAST(NULL AS STRING) as brand,
            CAST(NULL AS STRING) as manufacturer,
            CAST(NULL AS INT) as has_sku,
            CAST(NULL AS STRING) as variant_information,
            
            -- 分类信息
            category,
            sub_category,
            CAST(NULL AS STRING) as category_ids,
            
            -- 卖家信息
            seller_raw as seller,
            seller_id_raw as seller_id,
            
            -- 评价信息 (数据类型转换和清理)
            CASE 
                WHEN review_rating_raw = '' OR review_rating_raw = '0' THEN NULL
                ELSE review_rating_raw 
            END as review_rating,
            
            CASE 
                WHEN number_of_reviews_raw = '' OR number_of_reviews_raw = '0' THEN NULL
                ELSE cast(number_of_reviews_raw as bigint)
            END as number_of_reviews,
            
            -- 图片和描述
            CAST(NULL AS STRING) as first_image,
            CAST(NULL AS STRING) as imags,
            CAST(NULL AS STRING) as video,
            CAST(NULL AS STRING) as availability,
            CAST(NULL AS STRING) as inventory,
            CAST(NULL AS STRING) as comment_count_str,
            CAST(NULL AS BIGINT) as comment_count,
            CAST(NULL AS BIGINT) as vague_comment_count,
            CAST(NULL AS STRING) as specifications,
            CAST(NULL AS STRING) as additional_description,
            
            -- 任务相关字段
            batch_id,
            task_id,
            project,
            crawl_status,
            
            -- 额外信息 (JSON拼接，比UDF concat更高效)
            concat('{"expected_delivery_time":"', 
                   coalesce(expected_delivery_time, ''),
                   '","delivery_destination":"',
                   coalesce(delivery_destination, ''),
                   '"}') as extra_json,
            
            snapshot_time,
            
            -- 分区字段
            CASE 
                WHEN region_raw = '' THEN NULL
                ELSE upper(region_raw)  -- 提取出来后大写
            END as region,
            dt,
            etl_source
            
        FROM sales_count_parsed
        WHERE region_raw IS NOT NULL AND region_raw != ''
    )
    
    -- 最终查询
    SELECT 
        product_id, sku_id, product_title, url,
        sku_online_price, sku_online_price_currency, sku_list_price, sku_list_price_currency,
        promo, sell_count_str, sell_count, vague_sell_count, sell_count_type,
        color, size, brand, manufacturer, has_sku, variant_information,
        category, sub_category, category_ids,
        seller, seller_id,
        review_rating, number_of_reviews,
        first_image, imags, video, availability, inventory,
        comment_count_str, comment_count, vague_comment_count,
        specifications, additional_description,
        batch_id, task_id, project, crawl_status,
        extra_json, snapshot_time,
        region, dt, etl_source
    FROM final_transformed
    """
    
    # 使用简单的字符串替换避免f-string转义问题
    query = base_query.replace('CALC_PARTITION_PLACEHOLDER', calc_partition)
    query = query.replace('LIMIT_PLACEHOLDER', limit_clause)
    query = query.replace('TABLE_NAME_PLACEHOLDER', table_name)
    query = query.replace('ETL_SOURCE_PLACEHOLDER', etl_source)
    
    return spark.sql(query)

def dumper_menu_detail_v1(spark, calc_partition, table_config):
    limit_clause = f"LIMIT {TEST_LIMIT}" if TEST_LIMIT else ""
    table_name = table_config['table_name']
    etl_source = table_config['etl_source']
    
    base_query = """
    WITH base_data AS (
        -- 步骤1: ods_menu_detail_v1 基础数据提取
        SELECT 
            get_json_object(sr, '$.ulanzi|parent_asin') as product_id,
            retailer_product_code as sku_id,
            product_description as product_title,
            url,
            category,
            sub_category,
            batch_id,
            CAST(NULL AS STRING) as task_id,
            CAST(NULL AS STRING) as project,
            crawl_status,
            snapshot_time,
            dt,
            'ETL_SOURCE_PLACEHOLDER' as etl_source,
            -- 直接字段
            online_price,
            online_price_currency,
            list_price,
            promotion,
            color,
            size,
            brand,
            manufacturer,
            has_variants,
            variant_information,
            review_rating,
            number_of_reviews,
            product_image,
            secondary_image,
            aplus_images,
            video,
            availability,
            in_stock,
            specifications,
            additional_description,
            sr
        FROM TABLE_NAME_PLACEHOLDER
        WHERE dt = 'CALC_PARTITION_PLACEHOLDER'
        LIMIT_PLACEHOLDER
    ),
    
    json_extracted AS (
        -- 步骤2: ods_menu_detail_v1 JSON字段提取（较少）
        SELECT 
            *,
            -- 只需要提取少量JSON字段
            get_json_object(sr, '$.ulanzi|sales_count') as sell_count_str_raw,
            get_json_object(sr, '$.ulanzi|seller') as seller_raw,
            get_json_object(sr, '$.ulanzi|seller_id') as seller_id_raw,
            -- region按优先级提取
            CASE 
                WHEN get_json_object(sr, '$.ulanzi|region') IS NOT NULL 
                     AND get_json_object(sr, '$.ulanzi|region') != ''
                THEN get_json_object(sr, '$.ulanzi|region')
                WHEN regexp_extract(url, '^(https?://[^/]+)', 0) RLIKE '\\\\.[a-zA-Z]+'
                THEN upper(regexp_extract(regexp_extract(url, '^(https?://[^/]+)', 0), '\\\\.([a-zA-Z]+)', 1))
                ELSE 'US'
            END as region_raw,
            CAST(NULL AS STRING) as expected_delivery_time,
            CAST(NULL AS STRING) as delivery_destination
        FROM base_data
    ),
    
    sales_count_parsed AS (
        -- 步骤3: 销量数据解析
        SELECT 
            *,
            CASE 
                WHEN lower(sell_count_str_raw) RLIKE '\\\\d+\\\\.?\\\\d*k\\\\s*$'
                THEN cast(regexp_extract(lower(sell_count_str_raw), '(\\\\d+\\\\.?\\\\d*)', 1) as double) * 1000
                WHEN lower(sell_count_str_raw) RLIKE '\\\\d+\\\\.?\\\\d*w\\\\s*$'  
                THEN cast(regexp_extract(lower(sell_count_str_raw), '(\\\\d+\\\\.?\\\\d*)', 1) as double) * 10000
                WHEN sell_count_str_raw RLIKE '^\\\\d+\\\\.?\\\\d*$'
                THEN cast(sell_count_str_raw as double)
                ELSE NULL
            END as vague_sell_count_parsed
        FROM json_extracted
    ),
    
    final_transformed AS (
        -- 步骤4: ods_menu_detail_v1 最终字段映射
        SELECT 
            product_id,
            sku_id,
            product_title,
            url,
            
            -- 价格字段（直接字段）
            CASE 
                WHEN online_price = '' OR online_price = '0' THEN NULL
                ELSE online_price 
            END as sku_online_price,
            online_price_currency as sku_online_price_currency,
            
            CASE 
                WHEN list_price = '' OR list_price = '0' THEN NULL
                ELSE list_price 
            END as sku_list_price,
            -- 按优先级取币种
            CASE 
                WHEN get_json_object(sr, '$.ulanzi|online_price_currency') IS NOT NULL 
                     AND get_json_object(sr, '$.ulanzi|online_price_currency') != ''
                THEN get_json_object(sr, '$.ulanzi|online_price_currency')
                ELSE online_price_currency
            END as sku_list_price_currency,
            
            -- 促销和销量字段
            promotion as promo,
            CASE 
                WHEN sell_count_str_raw = '' THEN NULL
                ELSE sell_count_str_raw 
            END as sell_count_str,
            CAST(NULL AS BIGINT) as sell_count,
            
            CASE 
                WHEN vague_sell_count_parsed IS NOT NULL 
                THEN cast(vague_sell_count_parsed as string)
                ELSE NULL
            END as vague_sell_count,
            
            CASE 
                WHEN vague_sell_count_parsed IS NOT NULL 
                THEN 'last_30_count'
                ELSE NULL
            END as sell_count_type,
            
            -- 商品属性（直接字段）
            color,
            size,
            brand,
            manufacturer,
            CASE 
                WHEN has_variants = 'YES' THEN 1
                WHEN has_variants = 'NO' THEN 0
                ELSE NULL
            END as has_sku,
            variant_information,
            
            -- 分类信息
            category,
            sub_category,
            CAST(NULL AS STRING) as category_ids,
            
            -- 卖家信息（JSON提取）
            seller_raw as seller,
            seller_id_raw as seller_id,
            
            -- 评价信息（直接字段）
            CASE 
                WHEN review_rating = '' OR review_rating = '0' THEN NULL
                ELSE review_rating 
            END as review_rating,
            
            CASE 
                WHEN number_of_reviews = '' OR number_of_reviews = '0' THEN NULL
                ELSE cast(number_of_reviews as bigint)
            END as number_of_reviews,
            
            -- 图片和描述（直接字段）
            product_image as first_image,
            -- 合并图片字段
            CASE 
                WHEN secondary_image IS NOT NULL AND aplus_images IS NOT NULL
                THEN concat('[', 
                     regexp_replace(secondary_image, '\\\\|', ','), 
                     ',',
                     regexp_replace(aplus_images, '\\\\|', ','),
                     ']')
                WHEN secondary_image IS NOT NULL
                THEN concat('[', regexp_replace(secondary_image, '\\\\|', ','), ']')
                WHEN aplus_images IS NOT NULL  
                THEN concat('[', regexp_replace(aplus_images, '\\\\|', ','), ']')
                ELSE NULL
            END as imags,
            video,
            availability,
            in_stock as inventory,
            CAST(NULL AS STRING) as comment_count_str,
            CAST(NULL AS BIGINT) as comment_count,
            CAST(NULL AS BIGINT) as vague_comment_count,
            specifications,
            additional_description,
            
            -- 任务相关字段
            batch_id,
            task_id,
            project,
            crawl_status,
            
            -- 额外信息（该表没有delivery相关信息）
            '{}' as extra_json,
            
            snapshot_time,
            
            -- 分区字段
            upper(region_raw) as region,
            dt,
            etl_source
            
        FROM sales_count_parsed
        WHERE region_raw IS NOT NULL AND region_raw != ''
    )
    
    -- 最终查询
    SELECT 
        product_id, sku_id, product_title, url,
        sku_online_price, sku_online_price_currency, sku_list_price, sku_list_price_currency,
        promo, sell_count_str, sell_count, vague_sell_count, sell_count_type,
        color, size, brand, manufacturer, has_sku, variant_information,
        category, sub_category, category_ids,
        seller, seller_id,
        review_rating, number_of_reviews,
        first_image, imags, video, availability, inventory,
        comment_count_str, comment_count, vague_comment_count,
        specifications, additional_description,
        batch_id, task_id, project, crawl_status,
        extra_json, snapshot_time,
        region, dt, etl_source
    FROM final_transformed
    """
    
    # 使用简单的字符串替换避免f-string转义问题
    query = base_query.replace('CALC_PARTITION_PLACEHOLDER', calc_partition)
    query = query.replace('LIMIT_PLACEHOLDER', limit_clause)
    query = query.replace('TABLE_NAME_PLACEHOLDER', table_name)
    query = query.replace('ETL_SOURCE_PLACEHOLDER', etl_source)
    
    return spark.sql(query)

def dumper_all_tables(spark, calc_partition):
    """
    处理配置中指定的上游表并合并结果
    """
    all_dataframes = []
    
    for table_config in UPSTREAM_TABLES:
        # 检查是否需要处理这个表
        if table_config['table_type'] not in PROCESS_TABLES:
            print(f"Skipping table: {table_config['table_name']} (not in PROCESS_TABLES)")
            continue
            
        print(f"Processing table: {table_config['table_name']}")
        
        # 根据表类型选择对应的处理函数
        if table_config['table_type'] == 'menu_v1':
            df = dumper_menu_v1(spark, calc_partition, table_config)
        elif table_config['table_type'] == 'menu_detail_v1':
            df = dumper_menu_detail_v1(spark, calc_partition, table_config)
        else:
            raise ValueError(f"Unknown table type: {table_config['table_type']}")
            
        all_dataframes.append(df)
    
    # 检查是否有数据需要处理
    if len(all_dataframes) == 0:
        raise ValueError("No tables were processed. Check PROCESS_TABLES configuration.")
    
    # 如果只有一个表，直接返回
    if len(all_dataframes) == 1:
        return all_dataframes[0]
    
    # 如果有多个表，使用UNION ALL合并
    result_df = all_dataframes[0]
    for df in all_dataframes[1:]:
        result_df = result_df.unionAll(df)
    
    return result_df

# ==================== 数据加载 ====================
def loader(spark, df: DataFrame):
    """
    使用标准的spark_write_hive函数进行数据加载
    """
    spark_write_hive(df, spark, TARGET_DB, TARGET_TABLE, 
                     create_table_ddl=DDL, 
                     partition=None,  # 使用动态分区
                     emr=EMR,
                     refresh_stats=REFRESH_STATS, 
                     compression=COMPRESSION)

# ==================== 主函数 ====================
def main():
    """
    主执行函数 - SQL优化版本
    """
    init_logging()
    impala = new_impala_connector()
    # 创建连接
    spark = SparkSession.builder.appName("oneflow.{{dag_name}}.{{job_name}}").enableHiveSupport().getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    
    # 性能优化配置
    spark.sql(f'SET spark.sql.shuffle.partitions={PARTITION_NUM}')
    spark.sql('SET spark.sql.adaptive.enabled=true')
    spark.sql('SET spark.sql.adaptive.coalescePartitions.enabled=true')
    
    # 准备计算参数
    calc_partition = CALC_PARTITION
    
    # 数据提取和转换 (纯SQL实现) - 处理所有上游表
    df = dumper_all_tables(spark, calc_partition)
    
    # 数据加载
    loader(spark, df)
    
    # 关闭Spark会话
    spark.stop()
    impala.execute(f"invalidate metadata {TARGET_DB}.{TARGET_TABLE}")
    impala.execute(f"COMPUTE INCREMENTAL STATS {TARGET_DB}.{TARGET_TABLE}")

if __name__ == '__main__':
    exit(main())
