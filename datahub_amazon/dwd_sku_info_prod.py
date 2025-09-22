# -*- coding: utf-8 -*-
"""
Amazon数据ETL处理：ods_menu_v1和ods_menu_detail_v1 -> dwd_sku_info
支持多表源、动态分区、增强销量解析
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pigeon.utils import init_logging
from pigeon.connector import new_impala_connector
from dw_util.util import str_utils
from spark_util.insert import spark_write_hive

# ==================== 配置参数 ====================
TARGET_DB = 'datahub_amazon'
TARGET_TABLE = 'dwd_sku_info'
CALC_PARTITION = '{{ yesterday_ds }}'
REFRESH_STATS = False
EMR = True
PARTITION_NUM = 400
COMPRESSION = 'snappy'

# 表处理控制
PROCESS_TABLES = ['menu_v1', 'menu_detail_v1']
TEST_LIMIT = None

# 上游表配置
UPSTREAM_TABLES = [
    {
        'table_name': 'datahub_amazon.ods_menu_v1',
        'etl_source': 'datahub_amazon.ods_menu_v1',
        'table_type': 'menu_v1'
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
    vague_sell_count BIGINT,
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

QUERY_COLUMNS = str_utils.get_query_column_from_ddl(DDL)
PARTITION_DICT = None

# ==================== 销量解析逻辑 ====================
def get_sales_parsing_logic():
    """简化的销量解析逻辑，按需求实现"""
    return """
            CASE 
                -- 日本地区特殊处理：取"過去1か月で"后面和"点以上購入"前面的数字
                WHEN region_raw = 'JP' AND sell_count_str_raw LIKE '%過去1か月で%' AND sell_count_str_raw LIKE '%点以上購入%'
                THEN CASE 
                    WHEN regexp_extract(sell_count_str_raw, '過去1か月で([0-9]+\\\\.?[0-9]*).*点以上購入', 1) != ''
                    THEN cast(regexp_extract(sell_count_str_raw, '過去1か月で([0-9]+\\\\.?[0-9]*).*点以上購入', 1) as double)
                    ELSE NULL
                END
                -- 其他地区：取数字部分，k/mil乘以1000，w乘以10000
                WHEN region_raw != 'JP' AND regexp_extract(sell_count_str_raw, '([0-9]+\\\\.?[0-9]*)', 1) != ''
                THEN CASE 
                    WHEN lower(sell_count_str_raw) LIKE '%k%' OR lower(sell_count_str_raw) LIKE '%mil%'
                    THEN cast(regexp_extract(sell_count_str_raw, '([0-9]+\\\\.?[0-9]*)', 1) as double) * 1000
                    WHEN lower(sell_count_str_raw) LIKE '%w%'
                    THEN cast(regexp_extract(sell_count_str_raw, '([0-9]+\\\\.?[0-9]*)', 1) as double) * 10000
                    ELSE cast(regexp_extract(sell_count_str_raw, '([0-9]+\\\\.?[0-9]*)', 1) as double)
                END
                ELSE NULL
            END
    """

# ==================== 数据处理函数 ====================
def dumper_menu_v1(spark, calc_partition, table_config):
    """处理ods_menu_v1表"""
    limit_clause = f"LIMIT {TEST_LIMIT}" if TEST_LIMIT else ""
    table_name = table_config['table_name']
    etl_source = table_config['etl_source']
    
    sales_logic = get_sales_parsing_logic()
    
    base_query = """
    WITH base_data AS (
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
        SELECT 
            *,
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
        SELECT 
            *,
            SALES_LOGIC_PLACEHOLDER as vague_sell_count_parsed
        FROM json_extracted
    ),
    
    final_transformed AS (
        SELECT 
            product_id,
            sku_id,
            product_title,
            url,
            
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
            
            CAST(NULL AS STRING) as promo,
            CASE 
                WHEN sell_count_str_raw = '' THEN NULL
                ELSE sell_count_str_raw 
            END as sell_count_str,
            CAST(NULL AS BIGINT) as sell_count,
            
            CASE 
                WHEN vague_sell_count_parsed IS NOT NULL 
                THEN cast(round(vague_sell_count_parsed) as bigint)
                ELSE NULL
            END as vague_sell_count,
            
            CASE 
                WHEN vague_sell_count_parsed IS NOT NULL 
                THEN 'last_30_count'
                ELSE NULL
            END as sell_count_type,
            
            CAST(NULL AS STRING) as color,
            CAST(NULL AS STRING) as size,
            CAST(NULL AS STRING) as brand,
            CAST(NULL AS STRING) as manufacturer,
            CAST(NULL AS INT) as has_sku,
            CAST(NULL AS STRING) as variant_information,
            
            category,
            sub_category,
            CAST(NULL AS STRING) as category_ids,
            
            seller_raw as seller,
            seller_id_raw as seller_id,
            
            CASE 
                WHEN review_rating_raw = '' OR review_rating_raw = '0' THEN NULL
                ELSE review_rating_raw 
            END as review_rating,
            
            CASE 
                WHEN number_of_reviews_raw = '' OR number_of_reviews_raw = '0' THEN NULL
                ELSE cast(number_of_reviews_raw as bigint)
            END as number_of_reviews,
            
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
            
            batch_id,
            task_id,
            project,
            crawl_status,
            
            concat('{"expected_delivery_time":"', 
                   coalesce(expected_delivery_time, ''),
                   '","delivery_destination":"',
                   coalesce(delivery_destination, ''),
                   '"}') as extra_json,
            
            snapshot_time,
            
            CASE 
                WHEN region_raw = '' THEN NULL
                ELSE upper(region_raw)
            END as region,
            dt,
            etl_source
            
        FROM sales_count_parsed
        WHERE region_raw IS NOT NULL AND region_raw != ''
    )
    
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
    
    query = base_query.replace('CALC_PARTITION_PLACEHOLDER', calc_partition)
    query = query.replace('LIMIT_PLACEHOLDER', limit_clause)
    query = query.replace('TABLE_NAME_PLACEHOLDER', table_name)
    query = query.replace('ETL_SOURCE_PLACEHOLDER', etl_source)
    query = query.replace('SALES_LOGIC_PLACEHOLDER', sales_logic)
    
    return spark.sql(query)

def dumper_menu_detail_v1(spark, calc_partition, table_config):
    """处理ods_menu_detail_v1表"""
    limit_clause = f"LIMIT {TEST_LIMIT}" if TEST_LIMIT else ""
    table_name = table_config['table_name']
    etl_source = table_config['etl_source']
    
    sales_logic = get_sales_parsing_logic()
    
    base_query = """
    WITH base_data AS (
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
        SELECT 
            *,
            get_json_object(sr, '$.ulanzi|sales_count') as sell_count_str_raw,
            get_json_object(sr, '$.ulanzi|seller') as seller_raw,
            get_json_object(sr, '$.ulanzi|seller_id') as seller_id_raw,
            CASE 
                WHEN get_json_object(sr, '$.ulanzi|region') IS NOT NULL 
                     AND get_json_object(sr, '$.ulanzi|region') != ''
                THEN get_json_object(sr, '$.ulanzi|region')
                WHEN regexp_extract(url, '^(https?://[^/]+)', 0) RLIKE '\\\\.[a-zA-Z]+'
                THEN CASE 
                    WHEN regexp_extract(regexp_extract(url, '^(https?://[^/]+)', 0), '\\\\.([a-zA-Z]+)$', 1) = 'com'
                    THEN 'US'
                    ELSE upper(regexp_extract(regexp_extract(url, '^(https?://[^/]+)', 0), '\\\\.([a-zA-Z]+)$', 1))
                END
                ELSE 'US'
            END as region_raw,
            CAST(NULL AS STRING) as expected_delivery_time,
            CAST(NULL AS STRING) as delivery_destination
        FROM base_data
    ),
    
    sales_count_parsed AS (
        SELECT 
            *,
            SALES_LOGIC_PLACEHOLDER as vague_sell_count_parsed
        FROM json_extracted
    ),
    
    final_transformed AS (
        SELECT 
            product_id,
            sku_id,
            product_title,
            url,
            
            CASE 
                WHEN online_price = '' OR online_price = '0' THEN NULL
                ELSE online_price 
            END as sku_online_price,
            online_price_currency as sku_online_price_currency,
            
            CASE 
                WHEN list_price = '' OR list_price = '0' THEN NULL
                ELSE list_price 
            END as sku_list_price,
            CASE 
                WHEN get_json_object(sr, '$.ulanzi|online_price_currency') IS NOT NULL 
                     AND get_json_object(sr, '$.ulanzi|online_price_currency') != ''
                THEN get_json_object(sr, '$.ulanzi|online_price_currency')
                ELSE online_price_currency
            END as sku_list_price_currency,
            
            promotion as promo,
            CASE 
                WHEN sell_count_str_raw = '' THEN NULL
                ELSE sell_count_str_raw 
            END as sell_count_str,
            CAST(NULL AS BIGINT) as sell_count,
            
            CASE 
                WHEN vague_sell_count_parsed IS NOT NULL 
                THEN cast(round(vague_sell_count_parsed) as bigint)
                ELSE NULL
            END as vague_sell_count,
            
            CASE 
                WHEN vague_sell_count_parsed IS NOT NULL 
                THEN 'last_30_count'
                ELSE NULL
            END as sell_count_type,
            
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
            
            category,
            sub_category,
            CAST(NULL AS STRING) as category_ids,
            
            seller_raw as seller,
            seller_id_raw as seller_id,
            
            CASE 
                WHEN review_rating = '' OR review_rating = '0' THEN NULL
                ELSE review_rating 
            END as review_rating,
            
            CASE 
                WHEN number_of_reviews = '' OR number_of_reviews = '0' THEN NULL
                ELSE cast(number_of_reviews as bigint)
            END as number_of_reviews,
            
            product_image as first_image,
            CASE 
                WHEN secondary_image IS NOT NULL AND aplus_images IS NOT NULL
                THEN CASE 
                    -- 如果两个字段相同，只保留一个（避免完全重复）
                    WHEN secondary_image = aplus_images 
                    THEN concat('[\"', replace(secondary_image, ' | ', '\",\"'), '\"]')
                    -- 如果不同，简单合并（基础去重：避免字段级重复）
                    ELSE concat('[\"', 
                         replace(secondary_image, ' | ', '\",\"'), 
                         '\",\"',
                         replace(aplus_images, ' | ', '\",\"'),
                         '\"]')
                END
                WHEN secondary_image IS NOT NULL
                THEN concat('[\"', replace(secondary_image, ' | ', '\",\"'), '\"]')
                WHEN aplus_images IS NOT NULL  
                THEN concat('[\"', replace(aplus_images, ' | ', '\",\"'), '\"]')
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
            
            batch_id,
            task_id,
            project,
            crawl_status,
            
            '{}' as extra_json,
            
            snapshot_time,
            
            upper(region_raw) as region,
            dt,
            etl_source
            
        FROM sales_count_parsed
        WHERE region_raw IS NOT NULL AND region_raw != ''
    )
    
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
    
    query = base_query.replace('CALC_PARTITION_PLACEHOLDER', calc_partition)
    query = query.replace('LIMIT_PLACEHOLDER', limit_clause)
    query = query.replace('TABLE_NAME_PLACEHOLDER', table_name)
    query = query.replace('ETL_SOURCE_PLACEHOLDER', etl_source)
    query = query.replace('SALES_LOGIC_PLACEHOLDER', sales_logic)
    
    return spark.sql(query)

def dumper_all_tables(spark, calc_partition):
    """处理所有上游表并合并结果"""
    all_dataframes = []
    
    for table_config in UPSTREAM_TABLES:
        if table_config['table_type'] not in PROCESS_TABLES:
            continue
        
        if table_config['table_type'] == 'menu_v1':
            df = dumper_menu_v1(spark, calc_partition, table_config)
        elif table_config['table_type'] == 'menu_detail_v1':
            df = dumper_menu_detail_v1(spark, calc_partition, table_config)
        else:
            raise ValueError(f"Unknown table type: {table_config['table_type']}")
            
        all_dataframes.append(df)
    
    if len(all_dataframes) == 0:
        raise ValueError("No tables processed. Check PROCESS_TABLES configuration.")
    
    if len(all_dataframes) == 1:
        return all_dataframes[0]
    
    result_df = all_dataframes[0]
    for df in all_dataframes[1:]:
        result_df = result_df.unionAll(df)
    
    return result_df

def loader(spark, df: DataFrame, calc_partition):
    """数据加载 - 动态分区覆盖写入"""
    
    # 确保表存在
    spark.sql(DDL)
    
    # 创建临时视图
    df.createOrReplaceTempView("temp_dwd_sku_info")
    
    # 使用INSERT OVERWRITE动态分区写入
    overwrite_sql = f"""
    INSERT OVERWRITE TABLE {TARGET_DB}.{TARGET_TABLE}
    PARTITION (region, dt, etl_source)
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
    FROM temp_dwd_sku_info
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
    spark.sql('SET spark.sql.adaptive.enabled=true')
    spark.sql('SET spark.sql.adaptive.coalescePartitions=true')
    
    # 准备计算参数
    calc_partition = CALC_PARTITION
    
    # 数据提取和转换
    df = dumper_all_tables(spark, calc_partition)
    
    # 数据加载
    loader(spark, df, calc_partition)
    
    # 关闭Spark会话
    spark.stop()
    impala.execute(f"invalidate metadata {TARGET_DB}.{TARGET_TABLE}")
    impala.execute(f"DROP INCREMENTAL STATS {TARGET_DB}.{TARGET_TABLE} PARTITION(dt='{CALC_PARTITION}')")
    impala.execute(f"COMPUTE INCREMENTAL STATS {TARGET_DB}.{TARGET_TABLE}")

if __name__ == '__main__':
    exit(main())
