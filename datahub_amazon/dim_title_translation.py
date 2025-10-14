# -*- coding: utf-8 -*-
"""
Amazon商品标题翻译ETL脚本
主要功能：
1. 从符合条件的SKU中提取需要翻译的product_title和brand
2. 调用豆包API进行翻译
3. 实现翻译缓存机制，避免重复翻译
4. 控制翻译量级（每天不超过100万）
5. 输出到dim_title_translation表
"""

import json
import logging
import time
from typing import Dict, List, Optional
import requests
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pigeon.utils import init_logging
from pigeon.connector import new_impala_connector
from spark_util.insert import spark_write_hive
from recurvedata.connector import get_datasource_by_name

# ==================== 配置参数 ====================
# 目标数据库和表名
TARGET_DB = 'datahub_amazon'
TARGET_TABLE = 'dim_title_translation_test'
# 注意：由于翻译表是日度全量更新，不需要分区参数
# 是否在spark_write_hive中刷新元数据
REFRESH_STATS = False
# 是否为EMR环境
EMR = True
# 分区数
PARTITION_NUM = 400
# 压缩格式
COMPRESSION = 'snappy'

# 豆包API配置 - 从数据源配置获取
translation_ds = get_datasource_by_name('MS-Ulanzi-Translation')
translation_config = translation_ds.data
DOUBAO_API_KEY = translation_config['api_key']
DOUBAO_MODEL = translation_config['model']
DOUBAO_API_URL = translation_config['base_url'] + '/chat/completions'

# 翻译限制配置
MAX_DAILY_TRANSLATIONS = 1000000  # 4. 翻译量级需要+阈值，一天翻译不能超过100w
BATCH_SIZE = 100  # 批处理大小
REQUEST_DELAY = 0.1  # 请求间隔（秒）

# 测试限制（可选）
TEST_LIMIT = None  # 生产环境建议设为None

# ==================== 表结构DDL ====================
DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_DB}.{TARGET_TABLE} (
    `Fields` STRING COMMENT '需要翻译的字段',
    `Fields_in_translation` STRING COMMENT '翻译后的字段', 
    `Fields_hash`    STRING COMMENT '哈希（需要翻译的字段）'
) 
STORED AS PARQUET
"""


def init_logging():
    """初始化日志"""
    logging.basicConfig(
        level=logging.INFO,  # 显示INFO级别及以上的日志
        format='%(asctime)s - %(levelname)s - %(message)s'
    )


def call_doubao_api(product_title: str, brand: str, max_retries: int = 3) -> Optional[str]:
    """调用豆包API进行翻译 - 带重试机制"""
    for attempt in range(max_retries):
        try:
            # 1. 输入给翻译的参数 - 严格按照要求格式
            input_data = {
                "商品标题": product_title,
                "品牌": brand
            }
            
            # 2. 提示词 - 严格按照要求格式
            prompt = f"""
## 角色
精通中文、英文、德语、日文、西班牙语、葡萄牙的语言翻译

## 任务
将输入商品标题的内容翻译成纯中文。
语言可能是英文、德语、日文、西班牙语、葡萄牙语其中一种，需要根据实际情况进行甄别翻译。
如果对应商品标题原文里含有品牌对应文字则命中文字无需翻译，保留命中文字加翻译文本

## 输出要求
请直接输出翻译后的中文标题，不要包含任何其他内容。
"""
            
            headers = {
                'Authorization': f'Bearer {DOUBAO_API_KEY}',
                'Content-Type': 'application/json'
            }
            
            data = {
                'model': DOUBAO_MODEL,
                'messages': [
                    {
                        'role': 'user',
                        'content': f"{prompt}\n{json.dumps(input_data, ensure_ascii=False)}"
                    }
                ],
                'temperature': 0.1,
                'max_tokens': 1000
            }
            
            response = requests.post(DOUBAO_API_URL, headers=headers, json=data, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            if 'choices' in result and len(result['choices']) > 0:
                translation = result['choices'][0]['message']['content'].strip()
                return translation
            else:
                logging.error(f"API响应格式异常: {result}")
                return None
                
        except Exception as e:
            if attempt == max_retries - 1:
                logging.error(f"调用豆包API失败，已重试{max_retries}次: {e}")
                return None
            else:
                time.sleep(2 ** attempt)  # 指数退避
                continue
    
    return None

# ==================== 数据提取和转换 ====================
def dumper(spark: SparkSession) -> DataFrame:
    """
    使用纯SQL方式提取需要翻译的数据
    """
    limit_clause = f"LIMIT {TEST_LIMIT}" if TEST_LIMIT else ""
    
    # 先检查表是否存在，如果不存在则跳过缓存查询
    try:
        # 尝试查询缓存表，如果表不存在会抛出异常
        spark.sql(f"SELECT 1 FROM {TARGET_DB}.{TARGET_TABLE} LIMIT 1")
        table_exists = True
        logging.info(f"缓存表 {TARGET_DB}.{TARGET_TABLE} 存在，将进行缓存查询")
    except Exception:
        table_exists = False
        logging.info(f"缓存表 {TARGET_DB}.{TARGET_TABLE} 不存在，跳过缓存查询")
    
    if table_exists:
        # 表存在，使用完整的缓存查询
        query = f"""
        WITH max_etl_date AS (
            -- 第一步：获取最新ETL日期
            SELECT MAX(etl_dt) as max_etl_dt
            FROM {TARGET_DB}.crawler_masterlist
            WHERE task_type = 'amazon_translation'
        ),
        
        valid_skus AS (
            -- 第二步：获取符合条件的SKU
            SELECT sku_id
            FROM {TARGET_DB}.crawler_masterlist
            CROSS JOIN max_etl_date
            WHERE task_type = 'amazon_translation'
              AND etl_dt = max_etl_date.max_etl_dt
        ),
        
        translation_candidates AS (
            -- 第三步：获取符合条件的SKU数据
            SELECT DISTINCT
                b.product_title,
                b.brand,
                md5(b.product_title) as fields_hash
            FROM {TARGET_DB}.dim_sku_ori AS b
            LEFT SEMI JOIN valid_skus a ON a.sku_id = b.sku_id
            WHERE b.product_title IS NOT NULL 
            AND b.product_title != ''
            AND b.brand IS NOT NULL
            AND b.brand != ''
        ),
        
        cached_translations AS (
            -- 3. 翻译需要添加缓存，已经翻译过的无需请求大模型
            SELECT Fields_hash
            FROM {TARGET_DB}.{TARGET_TABLE}
            WHERE Fields_in_translation IS NOT NULL 
            AND Fields_in_translation != ''
        )
        
        SELECT 
            tc.product_title,
            tc.brand,
            tc.fields_hash
        FROM translation_candidates tc
        LEFT ANTI JOIN cached_translations ct ON tc.fields_hash = ct.Fields_hash
        {limit_clause}
        """
    else:
        # 表不存在，跳过缓存查询
        query = f"""
        WITH max_etl_date AS (
            -- 第一步：获取最新ETL日期
            SELECT MAX(etl_dt) as max_etl_dt
            FROM {TARGET_DB}.crawler_masterlist
            WHERE task_type = 'amazon_translation'
        ),
        
        valid_skus AS (
            -- 第二步：获取符合条件的SKU
            SELECT sku_id
            FROM {TARGET_DB}.crawler_masterlist
            CROSS JOIN max_etl_date
            WHERE task_type = 'amazon_translation'
              AND etl_dt = max_etl_date.max_etl_dt
        ),
        
        translation_candidates AS (
            -- 第三步：获取符合条件的SKU数据
            SELECT DISTINCT
                b.product_title,
                b.brand,
                md5(b.product_title) as fields_hash
            FROM {TARGET_DB}.dim_sku_ori AS b
            LEFT SEMI JOIN valid_skus a ON a.sku_id = b.sku_id
            WHERE b.product_title IS NOT NULL 
            AND b.product_title != ''
            AND b.brand IS NOT NULL
            AND b.brand != ''
        )
        
        SELECT 
            tc.product_title,
            tc.brand,
            tc.fields_hash
        FROM translation_candidates tc
        {limit_clause}
        """
    
    df = spark.sql(query)
    
    # 记录需要翻译的数据量
    data_count = df.count()
    logging.info(f"提取到需要翻译的数据量: {data_count} 条")
    
    return df

def process_translations_from_df(df: DataFrame) -> List[Dict]:
    translation_results = []
    
    # 收集数据到Driver节点
    rows = df.collect()
    
    # 批量处理，提高效率
    for i in range(0, len(rows), BATCH_SIZE):
        # 4. 翻译量级需要+阈值，一天翻译不能超过100w
        if i >= MAX_DAILY_TRANSLATIONS:
            logging.warning(f"达到每日翻译上限 {MAX_DAILY_TRANSLATIONS}，停止翻译")
            break
            
        batch = rows[i:i + BATCH_SIZE]
        
        for row in batch:
            product_title = row['product_title']
            brand = row['brand']
            fields_hash = row['fields_hash']
            
            # 调用翻译API
            translation = call_doubao_api(product_title, brand)
            
            if translation:
                translation_results.append({
                    'Fields': product_title,
                    'Fields_in_translation': translation,
                    'Fields_hash': fields_hash
                })
            
            # 请求间隔
            time.sleep(REQUEST_DELAY)
        
        # 批次间稍长间隔，避免API限制
        if i + BATCH_SIZE < len(rows):
            time.sleep(REQUEST_DELAY * 2)
    
    return translation_results


# ==================== 数据加载 ====================
# 预定义schema，避免重复创建
TRANSLATION_SCHEMA = StructType([
    StructField("Fields", StringType(), True),
    StructField("Fields_in_translation", StringType(), True),
    StructField("Fields_hash", StringType(), True)
])

def loader(spark: SparkSession, results: List[Dict]) -> None:
    """
    使用标准的spark_write_hive函数进行数据加载
    """
    if not results:
        return
    
    # 使用预定义schema创建DataFrame
    df = spark.createDataFrame(results, TRANSLATION_SCHEMA)
    
    # 写入Hive表 - 使用追加模式
    spark_write_hive(df, spark, TARGET_DB, TARGET_TABLE, 
                     create_table_ddl=DDL, 
                     partition=None,
                     emr=EMR,
                     refresh_stats=REFRESH_STATS, 
                     compression=COMPRESSION,
                     mode='INTO')

# ==================== 主函数 ====================
def main():
    init_logging()
    
    logging.info("开始执行Amazon商品标题翻译ETL任务")
    
    # 初始化连接
    impala = new_impala_connector()
    logging.info("Impala连接初始化完成")
    
    # 创建Spark会话
    spark = SparkSession.builder.appName("oneflow.{{dag_name}}.{{job_name}}").enableHiveSupport().getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    logging.info("Spark会话创建完成")
    
    # 性能优化配置
    spark.sql(f'SET spark.sql.shuffle.partitions={PARTITION_NUM}')
    spark.sql('SET spark.sql.adaptive.enabled=true')
    spark.sql('SET spark.sql.adaptive.coalescePartitions.enabled=true')
    logging.info("Spark性能配置完成")
    
    # 确保目标表存在
    logging.info("确保目标表存在...")
    try:
        spark.sql(DDL)
        logging.info(f"目标表 {TARGET_DB}.{TARGET_TABLE} 已确保存在")
    except Exception as e:
        logging.error(f"创建表失败: {e}")
        raise
    
    # 数据提取 - 使用纯SQL方式
    logging.info("开始数据提取...")
    df = dumper(spark)
    
    # 处理翻译
    logging.info("开始处理翻译...")
    translation_results = process_translations_from_df(df)
    
    # 数据加载
    logging.info("开始数据加载...")
    loader(spark, translation_results)
    
    logging.info(f"翻译任务完成，成功处理 {len(translation_results)} 条记录")
    
    # 资源清理
    spark.stop()
    impala.execute(f"invalidate metadata {TARGET_DB}.{TARGET_TABLE}")
    impala.execute(f"COMPUTE INCREMENTAL STATS {TARGET_DB}.{TARGET_TABLE}")
    logging.info("资源清理完成")

if __name__ == '__main__':
    main()