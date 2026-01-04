# -*- coding: utf-8 -*-
import logging

from pigeon.utils import init_logging
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from spark_util.insert import spark_write_hive

TARGET_DB = '{{ monthly_basic_info }}'.split('.')[0]
TARGET_TABLE = '{{ monthly_basic_info }}'.split('.')[1]
# 查询的分区，支持多种类型 Union[int, str, list, None]
# 表示"不过滤"的值: None, '', '*', 'ALL' (不区分大小写)
CALC_PARTITION = ['{{ month_dt }}', '{{ platform_filter }}', '{{ market_filter }}']
REFRESH_STATS = True
EMR = True
PARTITION_NUM = 600
COMPRESSION = 'gzip'

update_mode = '{{ update_mode }}'
project_start_month = '{{ project_start_month }}'
platform_lst = {{ platform_lst }}

DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_DB}.{TARGET_TABLE} (
    item_id          STRING,
    pfsku_id         STRING,
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
    manufacturer     STRING,
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
) PARTITIONED BY (platform STRING, month_dt STRING, market STRING) 
COMMENT '所有平台属性字段' 
STORED AS PARQUET
"""


def sql_with_log(sql, executor, params=None):
    logging.info(sql)
    if not params:
        return executor(sql)
    return executor(sql, params)


def _normalize_filter(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value or value.upper() in ('*', 'ALL', 'NULL', 'NONE'):
            return None
        return value
    return value


def dumper(spark, calc_partition, update_mode, project_start_month, platform_lst):
    # 解析分区参数
    if isinstance(calc_partition, list):
        month_dt = _normalize_filter(calc_partition[0] if len(calc_partition) > 0 else None)
        platform_filter = _normalize_filter(calc_partition[1] if len(calc_partition) > 1 else None)
        market_filter = _normalize_filter(calc_partition[2] if len(calc_partition) > 2 else None)
    else:
        month_dt = _normalize_filter(calc_partition)
        platform_filter = None
        market_filter = None
    
    # 构建基础 WHERE 条件（只包含上游表实际存在的字段）
    where_conditions = []
    if update_mode == 'incremental':
        if month_dt:
            where_conditions.append(f'month_dt = "{month_dt}"')
    elif update_mode == 'full':
        if month_dt:
            where_conditions.append(f'month_dt >= "{project_start_month}"')
        else:
            where_conditions.append('month_dt IS NOT NULL')
    
    base_where_clause = ' AND '.join(where_conditions) if where_conditions else '1=1'
    
    # 如果指定了 platform_filter，只处理对应的平台（因为 platform 是在 SELECT 中赋值的常量）
    filtered_platform_lst = [p for p in platform_lst if p == platform_filter] if platform_filter else list(platform_lst)
    
    # 构建平台查询列表
    platform_queries = []
    
    if 'Tmall' in filtered_platform_lst:
        # Tmall 的 market 固定为 'cn'，如果指定了 market_filter 且不是 'cn'，跳过
        if market_filter and market_filter.lower() != 'cn':
            pass
        else:
            platform_queries.append(f"""
        SELECT item_id,
               pfsku_id,
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
               CAST(NULL AS STRING) AS manufacturer,
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
               CAST(NULL AS STRING) AS category_6_cn,
               'Tmall' as platform,
               month_dt,
               'cn' AS market
        FROM {{ tmall_monthly_basic_info }}
        WHERE {base_where_clause}
        """)
    
    if 'Amazon' in filtered_platform_lst:
        # Amazon 的 market 来自上游表，可以在 WHERE 中过滤
        amazon_where_conditions = [base_where_clause]
        if market_filter:
            amazon_where_conditions.append(f'market = "{market_filter}"')
        amazon_where_clause = ' AND '.join(amazon_where_conditions)
        platform_queries.append(f"""
        SELECT CAST(item_id AS STRING) as item_id,
               CAST(pfsku_id AS STRING) as pfsku_id,
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
               manufacturer,
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
                CAST(NULL AS STRING) AS category_6_cn,
               'Amazon' as platform,
               month_dt,
               market
        FROM {{ amazon_monthly_basic_info }}
        WHERE {amazon_where_clause}
        """)
    
    if 'JD' in filtered_platform_lst:
        # JD 的 market 固定为 'cn'，如果指定了 market_filter 且不是 'cn'，跳过
        if market_filter and market_filter.lower() != 'cn':
            pass
        else:
            platform_queries.append(f"""
        SELECT item_id,
               item_id AS pfsku_id,
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
               CAST(NULL AS STRING) AS manufacturer,
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
               CAST(NULL AS STRING) AS category_6_cn,
               'JD' as platform,
               month_dt,
               'cn' AS market
        FROM {{ jd_monthly_basic_info }}
        WHERE {base_where_clause}
        """)
    
    if 'PDD' in filtered_platform_lst:
        # PDD 的 market 固定为 'cn'，如果指定了 market_filter 且不是 'cn'，跳过
        if market_filter and market_filter.lower() != 'cn':
            pass
        else:
            platform_queries.append(f"""
        SELECT item_id,
               item_id AS pfsku_id,
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
               CAST(NULL AS STRING) AS manufacturer,
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
               CAST(NULL AS STRING) AS category_6_cn,
               'PDD' as platform,
               month_dt,
               'cn' AS market
        FROM {{ pdd_monthly_basic_info }}
        WHERE {base_where_clause}
        """)
    
    if 'Suning' in filtered_platform_lst:
        # Suning 的 market 固定为 'cn'，如果指定了 market_filter 且不是 'cn'，跳过
        if market_filter and market_filter.lower() != 'cn':
            pass
        else:
            platform_queries.append(f"""
        SELECT item_id,
               item_id AS pfsku_id,
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
               CAST(NULL AS STRING) AS manufacturer,
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
               CAST(NULL AS STRING) AS category_6_cn,
               'Suning' as platform,
               month_dt,
               'cn' AS market
        FROM {{ suning_monthly_basic_info }}
        WHERE {base_where_clause}
        """)
    
    if 'Taobao' in filtered_platform_lst:
        # Taobao 的 market 固定为 'cn'，如果指定了 market_filter 且不是 'cn'，跳过
        if market_filter and market_filter.lower() != 'cn':
            pass
        else:
            platform_queries.append(f"""
        SELECT item_id,
               item_id AS pfsku_id,
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
               CAST(NULL AS STRING) AS manufacturer,
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
               CAST(NULL AS STRING) AS category_6_cn,
               'Taobao' as platform,
               month_dt,
               'cn' AS market
        FROM {{ taobao_monthly_basic_info }}
        WHERE {base_where_clause}
        """)
    
    if 'VIP' in filtered_platform_lst:
        # VIP 的 market 固定为 'cn'，如果指定了 market_filter 且不是 'cn'，跳过
        if market_filter and market_filter.lower() != 'cn':
            pass
        else:
            platform_queries.append(f"""
        SELECT item_id,
               item_id AS pfsku_id,
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
               CAST(NULL AS STRING) AS manufacturer,
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
               CAST(NULL AS STRING) AS category_6_cn,
               'VIP' as platform,
               month_dt,
               'cn' AS market
        FROM {{ vip_monthly_basic_info }}
        WHERE {base_where_clause}
        """)
    
    if 'Douyin' in filtered_platform_lst:
        # Douyin 的 market 固定为 'cn'，如果指定了 market_filter 且不是 'cn'，跳过
        if market_filter and market_filter.lower() != 'cn':
            pass
        else:
            platform_queries.append(f"""
        SELECT cast(item_id as string) as item_id,
               cast(item_id as string) AS pfsku_id,
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
               CAST(NULL AS STRING) AS manufacturer,
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
               CAST(NULL AS STRING) AS category_6_cn,
               'Douyin' as platform,
               month_dt,
               'cn' AS market
        FROM {{ douyin_monthly_basic_info }}
        WHERE {base_where_clause}
        """)
    
    if 'Kaola' in filtered_platform_lst:
        # Kaola 的 market 固定为 'cn'，如果指定了 market_filter 且不是 'cn'，跳过
        if market_filter and market_filter.lower() != 'cn':
            pass
        else:
            platform_queries.append(f"""
        SELECT cast(item_id as string) as item_id,
               cast(item_id as string) AS pfsku_id,
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
               CAST(NULL AS STRING) AS manufacturer,
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
               CAST(NULL AS STRING) AS category_6_cn,
               'Kaola' as platform,
               month_dt,
               'cn' AS market
        FROM {{ kaola_monthly_basic_info }}
        WHERE {base_where_clause}
        """)
    
    if 'Kuaishou' in filtered_platform_lst:
        # Kuaishou 的 market 固定为 'cn'，如果指定了 market_filter 且不是 'cn'，跳过
        if market_filter and market_filter.lower() != 'cn':
            pass
        else:
            platform_queries.append(f"""
        SELECT item_id,
               item_id AS pfsku_id,
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
               CAST(NULL AS STRING) AS manufacturer,
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
               CAST(NULL AS STRING) AS category_6_cn,
               'Kuaishou' as platform,
               month_dt,
               'cn' AS market
        FROM {{ kuaishou_monthly_basic_info }}
        WHERE {base_where_clause}
        """)
    
    # 添加 patch 表查询（patch 表有 platform 和 market 字段，可以在 WHERE 中过滤）
    patch_where_conditions = [base_where_clause]
    if platform_filter:
        patch_where_conditions.append(f'platform = "{platform_filter}"')
    if market_filter:
        patch_where_conditions.append(f'market = "{market_filter}"')
    patch_where_clause = ' AND '.join(patch_where_conditions)
    
    platform_queries.append(f"""
    SELECT item_id,
           pfsku_id,
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
           CAST(NULL AS STRING) AS manufacturer,
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
           sub_category,
           sub_category_cn,
           category_1_cn,
           category_2_cn,
           category_3_cn,
           category_4_cn,
           category_5_cn,
           category_6_cn,
           platform,
           month_dt,
           market
    FROM {{ monthly_patch }}
    WHERE {patch_where_clause}
    """)
    
    # 构建完整的 UNION ALL 查询
    query = " UNION ALL ".join(platform_queries)
    
    df = sql_with_log(query, spark.sql)
    
    return df


def loader(spark, df: DataFrame):
    spark_write_hive(df, spark, TARGET_DB, TARGET_TABLE, create_table_ddl=DDL, 
                     dynamic_partition='platform,month_dt,market', emr=EMR,
                     refresh_stats=REFRESH_STATS, compression=COMPRESSION, 
                     repartition_num=None)  # 使用自动计算，根据数据大小动态调整


def main():
    init_logging()
    spark = (SparkSession.builder.appName("rearc.{{dag_name}}.{{job_name}}.{{execution_date}}")
             .enableHiveSupport().getOrCreate())
    spark.sql(f'set spark.sql.shuffle.partitions={PARTITION_NUM}')
    spark.sql(f'set spark.sql.autoBroadcastJoinThreshold=-1')
    spark.sql(f'set spark.sql.broadcastTimeout=600')
    # 启用自适应执行（AQE）优化
    spark.conf.set('spark.sql.adaptive.enabled', 'true')
    spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled', 'true')

    # 查询数据
    df = dumper(spark, CALC_PARTITION, update_mode, project_start_month, platform_lst)
    # 写入数据
    loader(spark, df)
    
    spark.stop()
    

if __name__ == '__main__':
    exit(main())
