# -*- coding: utf-8 -*-
import logging

from dw_util.util import str_utils
from pigeon.connector import new_impala_connector
from pigeon.utils import init_logging
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from spark_util.insert import spark_write_hive

TARGET_DB = '{{ monthly_basic_info }}'.split('.')[0]
TARGET_TABLE = '{{ monthly_basic_info }}'.split('.')[1]
CALC_PARTITION = '{{ month_dt }}'
REFRESH_STATS = False
EMR = True
PARTITION_NUM = 600
COMPRESSION = 'gzip'

update_mode = '{{ update_mode }}'
project_start_month = '{{ project_start_month }}'
platform_lst = {{ platform_lst }}

if update_mode == 'incremental':
    month_field = ',month_dt'
    month_condition = f'month_dt = "{CALC_PARTITION}"'
    month_partition = f'month_dt = "{CALC_PARTITION}"'
    drop_incremental_stats = f'DROP INCREMENTAL STATS {TARGET_DB}.{TARGET_TABLE} PARTITION(month_dt="{CALC_PARTITION}");'
elif update_mode == 'full':
    month_field = ',month_dt'
    month_condition = 'month_dt IS NOT NULL'
    month_partition = 'month_dt'
    drop_incremental_stats = f'DROP INCREMENTAL STATS {TARGET_DB}.{TARGET_TABLE} PARTITION(month_dt>="{project_start_month}");'
else:
    raise ValueError(f'update_mode="{update_mode}" is invalid !!!')

DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_DB}.{TARGET_TABLE} (
    platform         STRING, 
    item_id          STRING,
    pfsku_id         STRING,
    market           STRING,
    item_title       STRING,
    pfsku_title      STRING,
    category_1       STRING,
    category_2       STRING,
    category_3       STRING,
    category_4       STRING,
    category_5       STRING,
    category_6       STRING,
    category_1_id    STRING,
    category_2_id    STRING,
    category_3_id    STRING,
    category_4_id    STRING,
    category_5_id    STRING,
    category_6_id    STRING,
    category_id      BIGINT,
    category_name    STRING,
    categories       STRING,
    shop_id          STRING,
    shop_name        STRING,
    shop_type        STRING,
    brand_id         STRING,
    brand_name       STRING,
    properties       STRING,
    shop_url         STRING,
    item_url         STRING,
    pfsku_url        STRING,
    item_image       STRING,
    item_images      STRING,
    pfsku_image      STRING,
    recommend_remark STRING,
    tags             STRING,
    basic_info       STRING,
    pfsku_title_cn   STRING,
    sub_category     STRING,
    sub_category_cn  STRING,
    category_1_cn    STRING,
    category_2_cn    STRING,
    category_3_cn    STRING,
    category_4_cn    STRING,
    category_5_cn    STRING,
    category_6_cn    STRING
) PARTITIONED BY (month_dt STRING) 
COMMENT '所有平台属性字段' 
STORED AS PARQUET
"""


def spark_sql_with_log(spark, query):
    logging.info(query)
    return spark.sql(query)


def dumper(spark):
    # 构建平台查询列表
    platform_queries = []
    
    if 'Tmall' in platform_lst:
        platform_queries.append(f"""
        SELECT 'Tmall' as platform,
               item_id,
               pfsku_id,
               'cn' AS market,
               item_title,
               pfsku_title,
               category_1,
               category_2,
               category_3,
               CAST(NULL AS STRING) AS category_4,
               CAST(NULL AS STRING) AS category_5,
               CAST(NULL AS STRING) AS category_6,
               CAST(NULL AS STRING) AS category_1_id,
               CAST(NULL AS STRING) AS category_2_id,
               CAST(NULL AS STRING) AS category_3_id,
               CAST(NULL AS STRING) AS category_4_id,
               CAST(NULL AS STRING) AS category_5_id,
               CAST(NULL AS STRING) AS category_6_id,
               category_id,
               category_name,
               categories,
               CAST(shop_id AS STRING) AS shop_id,
               shop_name,
               CAST(NULL AS STRING) AS shop_type,
               cast(brand_id as STRING) as brand_id,
               brand_name,
               properties,
               shop_url,
               item_url,
               pfsku_url,
               item_image,
               item_images,
               pfsku_image,
               CAST(NULL AS STRING) AS recommend_remark,
               tags,
               basic_info,
               CAST(NULL AS STRING) AS pfsku_title_cn,
               CAST(NULL AS STRING) AS sub_category,
               CAST(NULL AS STRING) AS sub_category_cn,
               CAST(NULL AS STRING) AS category_1_cn,
               CAST(NULL AS STRING) AS category_2_cn,
               CAST(NULL AS STRING) AS category_3_cn,
               CAST(NULL AS STRING) AS category_4_cn,
               CAST(NULL AS STRING) AS category_5_cn,
               CAST(NULL AS STRING) AS category_6_cn
               {month_field}
        FROM {{ tmall_monthly_basic_info }}
        WHERE {month_condition}
        """)
    
    if 'Amazon' in platform_lst:
        platform_queries.append(f"""
        SELECT 'Amazon' as platform,
               CAST(item_id AS STRING) as item_id,
               CAST(pfsku_id AS STRING) as pfsku_id,
               market,
               CAST(NULL AS STRING) AS item_title,
               pfsku_title,
               category_1,
               category_2,
               category_3,
               category_4,
               category_5,
               CAST(NULL AS STRING) AS category_6,
               CAST(NULL AS STRING) AS category_1_id,
               CAST(NULL AS STRING) AS category_2_id,
               CAST(NULL AS STRING) AS category_3_id,
               CAST(NULL AS STRING) AS category_4_id,
               CAST(NULL AS STRING) AS category_5_id,
               CAST(NULL AS STRING) AS category_6_id,
               CAST(NULL AS BIGINT) AS category_id,
               category_name,
               CAST(NULL AS STRING) AS categories,
               CAST(shop_id AS STRING) AS shop_id,
               shop_name,
               CAST(NULL AS STRING) AS shop_type,
               CAST(NULL AS STRING) AS brand_id,
               brand_name,
               CAST(NULL AS STRING) AS properties,
               CAST(NULL AS STRING) AS shop_url,
               CAST(NULL AS STRING) AS item_url,
               pfsku_url,
               CAST(NULL AS STRING) AS item_image,
               CAST(NULL AS STRING) AS item_images,
               pfsku_image,
               CAST(NULL AS STRING) AS recommend_remark,
               CAST(NULL AS STRING) AS tags,
               CAST(NULL AS STRING) AS basic_info,
               pfsku_title_cn,
               sub_category,
               sub_category_cn,
               category_1_cn,
               category_2_cn,
               category_3_cn,
               category_4_cn,
               category_5_cn,
               category_6_cn
               {month_field}
        FROM {{ amazon_monthly_basic_info }}
        WHERE {month_condition}
        """)
    
    if 'JD' in platform_lst:
        platform_queries.append(f"""
        SELECT 'JD' as platform,
               item_id,
               item_id AS pfsku_id,
               CAST(NULL AS STRING) AS market,
               item_title,
               pfsku_title,
               category_1,
               category_2,
               category_3,
               category_4,
               CAST(NULL AS STRING) AS category_5,
               CAST(NULL AS STRING) AS category_6,
               CAST(category_1_id AS STRING) AS category_1_id,
               CAST(category_2_id AS STRING) AS category_2_id,
               CAST(category_3_id AS STRING) AS category_3_id,
               CAST(category_4_id AS STRING) AS category_4_id,
               CAST(NULL AS STRING) AS category_5_id,
               CAST(NULL AS STRING) AS category_6_id,
               category_id,
               category_name,
               categories,
               CAST(shop_id AS STRING) AS shop_id,
               shop_name,
               store_type AS shop_type,
               cast (brand_id as STRING) as brand_id,
               brand_name,
               cast(NULL AS STRING) AS properties,
               shop_url,
               item_url,
               cast(NULL AS STRING) AS pfsku_url,
               item_image,
               item_images,
               cast(NULL AS STRING) AS pfsku_image,
               CAST(NULL AS STRING) AS recommend_remark,
               tags,
               basic_info,
               CAST(NULL AS STRING) AS pfsku_title_cn,
               CAST(NULL AS STRING) AS sub_category,
               CAST(NULL AS STRING) AS sub_category_cn,
               CAST(NULL AS STRING) AS category_1_cn,
               CAST(NULL AS STRING) AS category_2_cn,
               CAST(NULL AS STRING) AS category_3_cn,
               CAST(NULL AS STRING) AS category_4_cn,
               CAST(NULL AS STRING) AS category_5_cn,
               CAST(NULL AS STRING) AS category_6_cn
               {month_field}
        FROM {{ jd_monthly_basic_info }}
        WHERE {month_condition}
        """)
    
    if 'PDD' in platform_lst:
        platform_queries.append(f"""
        SELECT 'PDD' as platform,
               item_id,
               item_id AS pfsku_id,
               CAST(NULL AS STRING) AS market,
               item_title,
               CAST(NULL AS STRING) AS pfsku_title,
               category_1,
               category_2,
               category_3,
               category_4,
               CAST(NULL AS STRING) AS category_5,
               CAST(NULL AS STRING) AS category_6,
               CAST(NULL AS STRING) AS category_1_id,
               CAST(NULL AS STRING) AS category_2_id,
               CAST(NULL AS STRING) AS category_3_id,
               CAST(NULL AS STRING) AS category_4_id,
               CAST(NULL AS STRING) AS category_5_id,
               CAST(NULL AS STRING) AS category_6_id,
               category_id,
               category_name,
               categories,
               CAST(shop_id AS STRING) AS shop_id,
               shop_name,
               CAST(NULL AS STRING) AS shop_type,
               CAST(brand_id as STRING) AS brand_id,
               brand_name,
               cast(NULL AS STRING) AS properties,
               shop_url,
               item_url,
               cast(NULL AS STRING) AS pfsku_url,
               item_image,
               item_images,
               cast(NULL AS STRING) AS pfsku_image,
               CAST(NULL AS STRING) AS recommend_remark,
               tags,
               basic_info,
               CAST(NULL AS STRING) AS pfsku_title_cn,
               CAST(NULL AS STRING) AS sub_category,
               CAST(NULL AS STRING) AS sub_category_cn,
               CAST(NULL AS STRING) AS category_1_cn,
               CAST(NULL AS STRING) AS category_2_cn,
               CAST(NULL AS STRING) AS category_3_cn,
               CAST(NULL AS STRING) AS category_4_cn,
               CAST(NULL AS STRING) AS category_5_cn,
               CAST(NULL AS STRING) AS category_6_cn
               {month_field}
        FROM {{ pdd_monthly_basic_info }}
        WHERE {month_condition}
        """)
    
    if 'Suning' in platform_lst:
        platform_queries.append(f"""
        SELECT 'Suning' as platform,
               item_id,
               item_id AS pfsku_id,
               CAST(NULL AS STRING) AS market,
               item_title,
               CAST(NULL AS STRING) AS pfsku_title,
               category_1_name AS category_1,
               category_2_name AS category_2,
               category_3_name AS category_3,
               CAST(NULL AS STRING) AS category_4,
               CAST(NULL AS STRING) AS category_5,
               CAST(NULL AS STRING) AS category_6,
               CAST(category_1_id AS STRING) AS category_1_id,
               CAST(category_2_id AS STRING) AS category_2_id,
               CAST(category_3_id AS STRING) AS category_3_id,
               CAST(NULL AS STRING) AS category_4_id,
               CAST(NULL AS STRING) AS category_5_id,
               CAST(NULL AS STRING) AS category_6_id,
               category_3_id AS category_id,
               category_3_name AS category_name,
               categories,
               CAST(shop_id AS STRING) AS shop_id,
               shop_name,
               CAST(NULL AS STRING) AS shop_type,
               CAST(brand_id as STRING) AS brand_id,
               brand_name,
               cast(NULL AS STRING) AS properties,
               shop_url,
               item_url,
               cast(NULL AS STRING) AS pfsku_url,
               item_image,
               item_images,
               cast(NULL AS STRING) AS pfsku_image,
               CAST(NULL AS STRING) AS recommend_remark,
               tags,
               basic_info,
               CAST(NULL AS STRING) AS pfsku_title_cn,
               CAST(NULL AS STRING) AS sub_category,
               CAST(NULL AS STRING) AS sub_category_cn,
               CAST(NULL AS STRING) AS category_1_cn,
               CAST(NULL AS STRING) AS category_2_cn,
               CAST(NULL AS STRING) AS category_3_cn,
               CAST(NULL AS STRING) AS category_4_cn,
               CAST(NULL AS STRING) AS category_5_cn,
               CAST(NULL AS STRING) AS category_6_cn
               {month_field}
        FROM {{ suning_monthly_basic_info }}
        WHERE {month_condition}
        """)
    
    if 'Taobao' in platform_lst:
        platform_queries.append(f"""
        SELECT 'Taobao' as platform,
               item_id,
               item_id AS pfsku_id,
               'cn' AS market,
               item_title,
               CAST(NULL AS STRING) AS pfsku_title,
               category_1,
               category_2,
               category_3,
               CAST(NULL AS STRING) AS category_4,
               CAST(NULL AS STRING) AS category_5,
               CAST(NULL AS STRING) AS category_6,
               CAST(NULL AS STRING) AS category_1_id,
               CAST(NULL AS STRING) AS category_2_id,
               CAST(NULL AS STRING) AS category_3_id,
               CAST(NULL AS STRING) AS category_4_id,
               CAST(NULL AS STRING) AS category_5_id,
               CAST(NULL AS STRING) AS category_6_id,
               category_id,
               category_name,
               categories,
               CAST(shop_id AS STRING) AS shop_id,
               shop_name,
               CAST(NULL AS STRING) AS shop_type,
               cast (brand_id as STRING) as brand_id,
               brand_name,
               cast(NULL AS STRING) AS properties,
               shop_url,
               item_url,
               cast(NULL AS STRING) AS pfsku_url,
               item_image,
               item_images,
               cast(NULL AS STRING) AS pfsku_image,
               CAST(NULL AS STRING) AS recommend_remark,
               tags,
               basic_info,
               CAST(NULL AS STRING) AS pfsku_title_cn,
               CAST(NULL AS STRING) AS sub_category,
               CAST(NULL AS STRING) AS sub_category_cn,
               CAST(NULL AS STRING) AS category_1_cn,
               CAST(NULL AS STRING) AS category_2_cn,
               CAST(NULL AS STRING) AS category_3_cn,
               CAST(NULL AS STRING) AS category_4_cn,
               CAST(NULL AS STRING) AS category_5_cn,
               CAST(NULL AS STRING) AS category_6_cn
               {month_field}
        FROM {{ taobao_monthly_basic_info }}
        WHERE {month_condition}
        """)
    
    if 'VIP' in platform_lst:
        platform_queries.append(f"""
        SELECT 'VIP' as platform,
               item_id,
               item_id AS pfsku_id,
               CAST(NULL AS STRING) AS market,
               item_title,
               CAST(NULL AS STRING) AS pfsku_title,
               category_1,
               category_2,
               category_3,
               CAST(NULL AS STRING) AS category_4,
               CAST(NULL AS STRING) AS category_5,
               CAST(NULL AS STRING) AS category_6,
               CAST(category_1_id AS STRING) AS category_1_id,
               CAST(category_2_id AS STRING) AS category_2_id,
               CAST(category_3_id AS STRING) AS category_3_id,
               CAST(NULL AS STRING) AS category_4_id,
               CAST(NULL AS STRING) AS category_5_id,
               CAST(NULL AS STRING) AS category_6_id,
               category_id,
               category_name,
               categories,
               cast(NULL AS STRING) AS shop_id,
               cast(NULL AS STRING) AS shop_name,
               CAST(NULL AS STRING) AS shop_type,
               cast (brand_id as STRING) as brand_id,
               brand_name,
               cast(NULL AS STRING) AS properties,
               cast(NULL AS STRING) AS shop_url,
               item_url,
               cast(NULL AS STRING) AS pfsku_url,
               item_image,
               item_images,
               cast(NULL AS STRING) AS pfsku_image,
               CAST(NULL AS STRING) AS recommend_remark,
               tags,
               basic_info,
               CAST(NULL AS STRING) AS pfsku_title_cn,
               CAST(NULL AS STRING) AS sub_category,
               CAST(NULL AS STRING) AS sub_category_cn,
               CAST(NULL AS STRING) AS category_1_cn,
               CAST(NULL AS STRING) AS category_2_cn,
               CAST(NULL AS STRING) AS category_3_cn,
               CAST(NULL AS STRING) AS category_4_cn,
               CAST(NULL AS STRING) AS category_5_cn,
               CAST(NULL AS STRING) AS category_6_cn
               {month_field}
        FROM {{ vip_monthly_basic_info }}
        WHERE {month_condition}
        """)
    
    if 'Douyin' in platform_lst:
        platform_queries.append(f"""
        SELECT 'Douyin' as platform,
               cast(item_id as string) as item_id,
               cast(item_id as string) AS pfsku_id,
               CAST(NULL AS STRING) AS market,
               item_title,
               CAST(NULL AS STRING) AS pfsku_title,
               category_1_name as category_1,
               category_2_name as category_2,
               category_3_name as category_3,
               category_4_name as category_4,
               CAST(NULL AS STRING) AS category_5,
               CAST(NULL AS STRING) AS category_6,
               CAST(category_1_id AS STRING) AS category_1_id,
               CAST(category_2_id AS STRING) AS category_2_id,
               CAST(category_3_id AS STRING) AS category_3_id,
               CAST(category_4_id AS STRING) AS category_4_id,
               CAST(NULL AS STRING) AS category_5_id,
               CAST(NULL AS STRING) AS category_6_id,
               category_4_id as category_id,
               category_4_name as category_name,
               categories,
               store_id as shop_id,
               shop_name,
               CAST(NULL AS STRING) AS shop_type,
               cast (null as STRING) as brand_id,
               brand_name,
               cast(NULL AS STRING) AS properties,
               shop_url,
               item_url,
               cast(NULL AS STRING) AS pfsku_url,
               item_image,
               item_images,
               cast(NULL AS STRING) AS pfsku_image,
               recommend_remark,
               tags,
               basic_info,
               CAST(NULL AS STRING) AS pfsku_title_cn,
               CAST(NULL AS STRING) AS sub_category,
               CAST(NULL AS STRING) AS sub_category_cn,
               CAST(NULL AS STRING) AS category_1_cn,
               CAST(NULL AS STRING) AS category_2_cn,
               CAST(NULL AS STRING) AS category_3_cn,
               CAST(NULL AS STRING) AS category_4_cn,
               CAST(NULL AS STRING) AS category_5_cn,
               CAST(NULL AS STRING) AS category_6_cn
               {month_field}
        FROM {{ douyin_monthly_basic_info }}
        WHERE {month_condition}
        """)
    
    if 'Kaola' in platform_lst:
        platform_queries.append(f"""
        SELECT 'Kaola' as platform,
               cast(item_id as string) as item_id,
               cast(item_id as string) AS pfsku_id,
               CAST(NULL AS STRING) AS market,
               item_title,
               CAST(NULL AS STRING) AS pfsku_title,
               category_1 as category_1,
               category_2 as category_2,
               category_3 as category_3,
               category_4 as category_4,
               category_5 as category_5,
               category_6 as category_6,
               CAST(category_1_id AS STRING) AS category_1_id,
               CAST(category_2_id AS STRING) AS category_2_id,
               CAST(category_3_id AS STRING) AS category_3_id,
               CAST(category_4_id AS STRING) AS category_4_id,
               CAST(category_5_id AS STRING) AS category_5_id,
               CAST(category_6_id AS STRING) AS category_6_id,
               category_id,
               category_name,
               CAST(NULL AS STRING) AS categories,
               CAST(shop_id AS STRING) AS shop_id,
               shop_name,
               CAST(NULL AS STRING) AS shop_type,
               CAST(brand_id as STRING) AS brand_id,
               brand_name,
               cast(NULL AS STRING) AS properties,
               cast(NULL AS STRING) AS shop_url,
               item_url,
               cast(NULL AS STRING) AS pfsku_url,
               cast(NULL AS STRING) AS item_image,
               cast(NULL AS STRING) AS item_images,
               cast(NULL AS STRING) AS pfsku_image,
               cast(NULL AS STRING) AS recommend_remark,
               tags,
               basic_info,
               CAST(NULL AS STRING) AS pfsku_title_cn,
               CAST(NULL AS STRING) AS sub_category,
               CAST(NULL AS STRING) AS sub_category_cn,
               CAST(NULL AS STRING) AS category_1_cn,
               CAST(NULL AS STRING) AS category_2_cn,
               CAST(NULL AS STRING) AS category_3_cn,
               CAST(NULL AS STRING) AS category_4_cn,
               CAST(NULL AS STRING) AS category_5_cn,
               CAST(NULL AS STRING) AS category_6_cn
               {month_field}
        FROM {{ kaola_monthly_basic_info }}
        WHERE {month_condition}
        """)
    
    if 'Kuaishou' in platform_lst:
        platform_queries.append(f"""
        SELECT 'Kuaishou' as platform,
               item_id,
               item_id AS pfsku_id,
               CAST(NULL AS STRING) AS market,
               item_title,
               CAST(NULL AS STRING) AS pfsku_title,
               category_1,
               category_2,
               category_3,
               CAST(NULL AS STRING) AS category_4,
               CAST(NULL AS STRING) AS category_5,
               CAST(NULL AS STRING) AS category_6,
               CAST(NULL AS STRING) AS category_1_id,
               CAST(NULL AS STRING) AS category_2_id,
               CAST(NULL AS STRING) AS category_3_id,
               CAST(NULL AS STRING) AS category_4_id,
               CAST(NULL AS STRING) AS category_5_id,
               CAST(NULL AS STRING) AS category_6_id,
               CAST(NULL AS BIGINT) as category_id,
               category_name,
               cast(NULL AS STRING) AS categories,
               CAST(shop_id AS STRING) AS shop_id,
               shop_name,
               shop_type,
               cast (null as STRING) as brand_id,
               brand_name,
               cast(NULL AS STRING) AS properties,
               cast(NULL AS STRING) AS shop_url,
               cast(NULL AS STRING) AS item_url,
               cast(NULL AS STRING) AS pfsku_url,
               cast(NULL AS STRING) AS item_image,
               cast(NULL AS STRING) AS item_images,
               cast(NULL AS STRING) AS pfsku_image,
               cast(NULL AS STRING) AS recommend_remark,
               cast(NULL AS STRING) AS tags,
               cast(NULL AS STRING) AS basic_info,
               CAST(NULL AS STRING) AS pfsku_title_cn,
               CAST(NULL AS STRING) AS sub_category,
               CAST(NULL AS STRING) AS sub_category_cn,
               CAST(NULL AS STRING) AS category_1_cn,
               CAST(NULL AS STRING) AS category_2_cn,
               CAST(NULL AS STRING) AS category_3_cn,
               CAST(NULL AS STRING) AS category_4_cn,
               CAST(NULL AS STRING) AS category_5_cn,
               CAST(NULL AS STRING) AS category_6_cn
               {month_field}
        FROM {{ kuaishou_monthly_basic_info }}
        WHERE {month_condition}
        """)
    
    # 添加 patch 表查询
    platform_queries.append(f"""
    SELECT platform,
           item_id,
           pfsku_id,
           CAST(NULL AS STRING) AS market,
           item_title,
           pfsku_title,
           category_1,
           category_2,
           category_3,
           category_4,
           category_5,
           category_6,
           category_1_id,
           category_2_id,
           category_3_id,
           category_4_id,
           category_5_id,
           category_6_id,
           category_id,
           category_name,
           categories,
           CAST(shop_id AS STRING) AS shop_id,
           shop_name,
           shop_type,
           brand_id,
           brand_name,
           properties,
           shop_url,
           item_url,
           pfsku_url,
           item_image,
           item_images,
           pfsku_image,
           recommend_remark,
           tags,
           basic_info,
           CAST(NULL AS STRING) AS pfsku_title_cn,
           CAST(NULL AS STRING) AS sub_category,
           CAST(NULL AS STRING) AS sub_category_cn,
           CAST(NULL AS STRING) AS category_1_cn,
           CAST(NULL AS STRING) AS category_2_cn,
           CAST(NULL AS STRING) AS category_3_cn,
           CAST(NULL AS STRING) AS category_4_cn,
           CAST(NULL AS STRING) AS category_5_cn,
           CAST(NULL AS STRING) AS category_6_cn
           {month_field}
    FROM {{ monthly_patch }}
    WHERE {month_condition}
    """)
    
    # 构建完整的 UNION ALL 查询
    query = " UNION ALL ".join(platform_queries)
    
    df = spark_sql_with_log(spark, query)
    return df


def loader(spark, df: DataFrame):
    spark_write_hive(df, spark, TARGET_DB, TARGET_TABLE, create_table_ddl=DDL, dynamic_partition='month_dt', emr=EMR,
                     refresh_stats=REFRESH_STATS, compression=COMPRESSION, repartition_num=PARTITION_NUM)


def main():
    init_logging()
    impala = new_impala_connector(emr=True)
    spark = (SparkSession.builder.appName("rearc.{{dag_name}}.{{job_name}}.{{execution_date}}")
             .enableHiveSupport().getOrCreate())
    spark.sql(f'set spark.sql.shuffle.partitions={PARTITION_NUM}')
    spark.sql(f'set spark.sql.autoBroadcastJoinThreshold=-1')
    spark.sql(f'set spark.sql.broadcastTimeout=600')

    df = dumper(spark)
    loader(spark, df)
    
    spark.stop()
    impala.execute(f'REFRESH {TARGET_DB}.{TARGET_TABLE} PARTITION({month_partition})')
    impala.execute(drop_incremental_stats)
    impala.execute(f'COMPUTE INCREMENTAL STATS {TARGET_DB}.{TARGET_TABLE} PARTITION({month_partition})')
    

if __name__ == '__main__':
    exit(main())
