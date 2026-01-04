# -*- coding: utf-8 -*-
"""
主要逻辑思路：
1. 从dwd_aigc_result_data_dev表读取数据
2. 筛选出需要合并的套装数据（排除特定品类，且多sku_no的）
3. 对电热蚊香液套装按std_brand_name分组合并（使用Spark SQL + 少量UDF）
4. 对一次性马桶刷套装按std_brand_name分组合并（使用Spark SQL + 少量UDF）
5. 对其他品类只保留sku_no=1的数据
6. 将处理后的数据与不需要处理的数据合并
7. 应用规则集清洗（品类、属性等）
8. 写入目标表
"""

import collections
import copy
import json
import logging
import re

from collections import OrderedDict
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, row_number, when, sum as spark_sum, concat_ws, collect_list, first, min as spark_min, lower
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pigeon.connector import new_impala_connector
from pigeon.utils import init_logging
from spark_util.insert import spark_write_hive
from ymrbdt.attributes import read_rule_set

# ==================== 配置参数 ====================
SOURCE_TABLE = '{{ dwd_aigc_result_data}}'
TARGET_DB = '{{scj_pfsku_package_splited}}'.split('.')[0] if '.' in '{{scj_pfsku_package_splited}}' else 'ms_scj_alijd'
TARGET_TABLE = '{{scj_pfsku_package_splited}}'.split('.')[-1] if '.' in '{{scj_pfsku_package_splited}}' else '{{scj_pfsku_package_splited}}'
UPDATE_MODE = '{{ update_mode }}'
MONTH_DT = '{{ month_dt }}'
PROJECT_START_MONTH = '{{ project_start_month }}'
EMR = False
PARTITION_NUM = 200
COMPRESSION = 'snappy'
REFRESH_STATS = True

# 增量更新 或者 全量更新
if UPDATE_MODE == 'incremental':
    MONTH_CONDITION = f'month_dt = "{MONTH_DT}"'
elif UPDATE_MODE == 'full':
    MONTH_CONDITION = '1=1'
else:
    raise ValueError(f'update_mode="{UPDATE_MODE}" is invalid !!!')

# 规则集配置 - 按照执行顺序配置
rule_set_dict = {
    'std_category': (104985, read_rule_set(104985),),  
    'form': (104986, read_rule_set(104986),),
}

DDL = f'''
CREATE TABLE IF NOT EXISTS {TARGET_DB}.{TARGET_TABLE} (
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
    top80 DOUBLE,
    month_start STRING,
    std_category_1 STRING,
    std_category_2 STRING,
    std_category_3 STRING,
    std_category_src STRING,
    std_brand_name STRING,
    std_spu_name STRING,
    flavor STRING,
    std_group_spu_name STRING,
    is_bundle INT,
    is_gift INT,
    sku_no INT,
    bundle_name STRING,
    weight DOUBLE,
    weight_unit STRING,
    package_num INT,
    package_unit STRING,
    unit_weight DOUBLE,
    quantity INT,
    quantity_unit STRING,
    form STRING,
    target_format STRING,
    attribute_src STRING,
    pfsku_value_sales DOUBLE,
    pfsku_unit_sales INT,
    pfsku_discount_price DOUBLE,
    top80_category DOUBLE,
    ingredient  STRING,
    sku_no_ori  INT
) PARTITIONED BY (month_dt STRING) 
COMMENT 'SCJ子产品包拆分表' 
STORED AS PARQUET
'''


def get_mapping_result_and_src(row, rule_set_key):
    rule_set_id = rule_set_dict[rule_set_key][0]
    rule_set = rule_set_dict[rule_set_key][1]

    rv = rule_set.transform(row)[0]
    value_dct = {}
    for col_name in rule_set.result_column:
        value_dct[col_name] = rv[col_name]

    if rv['__matched_rule'] == '__DEFAULT__':
        priority = '__DEFAULT__'
    else:
        if isinstance(rv['__matched_rule'], list):
            priority = ','.join([str(json.loads(x).get('priority')) for x in rv['__matched_rule']])
        else:
            priority = json.loads(rv['__matched_rule'])['priority']

    src = f'{rule_set_id},{priority}'
    return value_dct, src


def transform(row):
    # 标准品类清洗 - 处理std_category_1, std_category_2, std_category_3, std_category_4
    value, src = get_mapping_result_and_src(row, 'std_category')
    for i in range(1, 5):  # std_category_1, std_category_2, std_category_3, std_category_4
        if f'std_category_{i}' in value and value[f'std_category_{i}'] not in [None, 'NULL', '']:
            row[f'std_category_{i}'] = value[f'std_category_{i}']
            row['std_category_src'] = src
    
    # 产品属性清洗 - 依赖规则集104986，只保留有值的字段
    value, src = get_mapping_result_and_src(row, 'form')
    attribute_fields = ['form', 'target_format']
    
    # 只保留有值的字段进行赋值，规则集104986结果会覆盖上游字段值
    for field in attribute_fields:
        if field in value and value[field] not in [None, 'NULL', '']:
            row[field] = value[field]
            row['attribute_src'] = src
    
    return row


def _spark_row_to_ordereddict(row):
    """将Spark Row转换为OrderedDict"""
    return collections.OrderedDict(zip(row.__fields__, row))


def apply_transformation(rows):
    """应用数据转换，确保返回的Row对象字段顺序正确"""
    from pyspark.sql import Row
    
    for row in rows:
        # 获取原始字段名列表（保持顺序）
        field_names = row.__fields__
        
        # 转换为OrderedDict
        row_dict = _spark_row_to_ordereddict(row)
        transformed_dict = transform(row_dict)
        # 如果转换后字典缺少字段，使用原始值而不是None
        ordered_values = []
        for field in field_names:
            if field in transformed_dict:
                ordered_values.append(transformed_dict[field])
            else:
                ordered_values.append(row_dict.get(field))
        
        yield tuple(ordered_values)


# ==================== 工具函数 ====================
def safe_int(s):
    """安全的整数转换"""
    if s is None:
        return s
    return int(s)


def safe_float(s):
    """安全的浮点数转换"""
    if s is None:
        return s
    return float(s)


def safe_round(f):
    """安全的四舍五入"""
    if f is None:
        return f
    return round(f)


# ==================== UDF函数（用于复杂字符串拼接） ====================
def merge_mosquito_spu_name_udf(liquid_info_list):
    """
    合并电热蚊香液的std_spu_name
    参数：liquid_info_list - JSON字符串数组，每个元素是{"weight":..., "package_num":..., "std_spu_name":...}
    返回：合并后的std_spu_name字符串
    """
    if not liquid_info_list:
        return None
    
    try:
        import json
        # liquid_info_list是数组，每个元素是JSON字符串
        if isinstance(liquid_info_list, list):
            items = [json.loads(item) if isinstance(item, str) else item for item in liquid_info_list]
        elif isinstance(liquid_info_list, str):
            items = json.loads(liquid_info_list)
        else:
            items = liquid_info_list
        
        # 分离纯液和器的数据
        liquid_items = {}  # {weight: package_num_sum}
        device_package_num = 0
        base_spu_name = None
        min_sku_no = None
        base_has_weight = False  # 记录当前base是否有weight
        
        for item in items:
            sku_no = item.get('sku_no')
            weight = item.get('weight')
            package_num = item.get('package_num', 0) or 0
            std_spu_name = str(item.get('std_spu_name', '') or '')
            
            # 明确区分：纯液（只包含"液"不包含"器"）、器（包含"器"）
            is_pure_liquid = '液' in std_spu_name and '器' not in std_spu_name
            is_device = '器' in std_spu_name
            
            if is_pure_liquid:
                # 处理纯液
                has_weight = weight is not None and weight != ''
                if has_weight:
                    try:
                        weight = float(weight)
                        if weight in liquid_items:
                            liquid_items[weight] += package_num
                        else:
                            liquid_items[weight] = package_num
                    except (ValueError, TypeError):
                        has_weight = False
                
                # 选择纯液作为base，优先级：1)有weight 2)sku_no小
                should_update_base = False
                if base_spu_name is None:
                    # 还没有base，直接设置
                    should_update_base = True
                elif has_weight and not base_has_weight:
                    # 当前有weight，base没有 → 更新
                    should_update_base = True
                elif has_weight == base_has_weight and sku_no is not None and (min_sku_no is None or sku_no < min_sku_no):
                    # weight状态相同，比较sku_no
                    should_update_base = True
                
                if should_update_base:
                    base_spu_name = re.sub(r'\d+ml.*', '', std_spu_name).strip()
                    min_sku_no = sku_no
                    base_has_weight = has_weight
            elif is_device:
                # 处理器（包括"液加热器"这种同时包含液和器的）
                device_package_num += package_num
        
        if device_package_num == 0:
            device_package_num = 1
        
        # 构建液的克重*件数字符串
        liquid_parts = []
        for weight in sorted(liquid_items.keys()):
            package_num = liquid_items[weight]
            # weight 四舍五入保留整数
            rounded_weight = round(weight)
            liquid_parts.append(f'{rounded_weight}ml*{package_num}')
        liquid_str = '+'.join(liquid_parts) if liquid_parts else ''
        
        # 合并std_spu_name
        if base_spu_name:
            if liquid_str:
                return f'{base_spu_name}{liquid_str}+{device_package_num}器'
            else:
                return f'{base_spu_name}{device_package_num}器'
        return None
    except Exception:
        return None


def merge_toilet_brush_spu_name_udf(brush_info_list):
    """
    合并一次性马桶刷的std_spu_name
    参数：brush_info_list - JSON字符串数组，每个元素是{"package_num":..., "std_spu_name":...}
    返回：合并后的std_spu_name字符串
    """
    if not brush_info_list:
        return None
    
    try:
        import json
        # brush_info_list是数组，每个元素是JSON字符串
        if isinstance(brush_info_list, list):
            items = [json.loads(item) if isinstance(item, str) else item for item in brush_info_list]
        elif isinstance(brush_info_list, str):
            items = json.loads(brush_info_list)
        else:
            items = brush_info_list
        
        package_num1 = 0  # 不包含杆或柄的（主体）
        package_num2 = 0  # 包含杆或柄的（配件：刷杆、标准杆、长杆等）
        base_spu_name = None
        min_sku_no = None
        
        for item in items:
            sku_no = item.get('sku_no')
            package_num = item.get('package_num', 0) or 0
            std_spu_name = str(item.get('std_spu_name', '') or '')
            has_handle = '杆' in std_spu_name or '柄' in std_spu_name  # 包含"杆"或"柄"的都是配件（刷杆、标准杆、长杆等）
            
            if has_handle:
                package_num2 += package_num
            else:
                package_num1 += package_num
                # 选择sku_no最小的不含杆或柄的作为base
                if base_spu_name is None or (sku_no is not None and (min_sku_no is None or sku_no < min_sku_no)):
                    base_spu_name = std_spu_name.strip()
                    min_sku_no = sku_no
        
        if package_num2 == 0:
            package_num2 = 1
        
        if base_spu_name:
            return f'{base_spu_name}*{package_num1}+{package_num2}'
        return None
    except Exception:
        return None


# ==================== 数据查询函数 ====================
def dumper(spark, calc_partition):
    # 基础查询
    base_query = f'''
    SELECT s.platform,
           s.item_id,
           s.pfsku_id,
           s.item_title,
           s.pfsku_title,
           s.first_image,
           s.pfsku_image,
           s.category_name,
           s.shop_name,
           s.brand_name,
           s.item_url,
           s.pfsku_url,
           s.tags,
           s.top80,
           s.month_start,
           s.std_category_1,
           s.std_category_2,
           s.std_category_3,
           s.std_category_4,
           cast(null as string) as std_category_src,
           s.std_brand_name,
           s.std_spu_name,
           s.flavor,
           s.std_group_spu_name,
           s.is_bundle,
           s.is_gift,
           s.sku_no,
           s.bundle_name,
           s.weight,
           s.weight_unit,
           s.package_num,
           s.package_unit,
           s.unit_weight,
           s.quantity,
           s.quantity_unit,
           s.form,
           s.need_toilet_brush,
           s.need_with_water,
           s.use_method,
           s.spray_form,
           s.texture,
           s.benefit,
           s.target_user,
           s.product_scenarios,
           s.disinfectant_claim,
           cast(null as string) as target_format,
           cast(null as string) as attribute_src,
           s.pfsku_value_sales,
           s.pfsku_unit_sales,
           s.pfsku_discount_price,
           s.top80_category,
           s.ingredient,
           s.sku_no as sku_no_ori,
           s.month_dt
    FROM {SOURCE_TABLE} s
    WHERE {MONTH_CONDITION}
    '''
    
    df_base = spark.sql(base_query)
    df_base.createOrReplaceTempView('tv_base_data')
    
    FILTER_SOURCE_TABLE = 'ms_scj_alijd.home_clean_aigc_result_data_ori_dev'
    filter_query = f'''
    WITH t0 AS (
        SELECT platform, item_id, pfsku_id, month_dt
        FROM {FILTER_SOURCE_TABLE}
        WHERE lower(std_category_2) RLIKE '厨房油污清洁剂|杀虫气雾剂|驱蚊'
        GROUP BY platform, item_id, pfsku_id, month_dt
    ),
    t1 AS (
        SELECT 
            a.platform, a.item_id, a.pfsku_id, a.month_dt,
            COUNT(DISTINCT a.sku_no) AS sku_num,
            CONCAT_WS(',', COLLECT_SET(a.std_category_3)) AS categorys
        FROM {FILTER_SOURCE_TABLE} a
        LEFT ANTI JOIN t0 
            ON a.platform = t0.platform 
            AND a.item_id = t0.item_id 
            AND a.pfsku_id = t0.pfsku_id 
            AND a.month_dt = t0.month_dt
        WHERE {MONTH_CONDITION}
        GROUP BY a.platform, a.item_id, a.pfsku_id, a.month_dt
        HAVING COUNT(DISTINCT a.sku_no) > 1
    ),
    t2 AS (
        SELECT platform, item_id, pfsku_id, month_dt, categorys
        FROM t1
        WHERE lower(categorys) NOT RLIKE ',|洁厕|others'
    )
    SELECT 
        b.*,
        t2.categorys
    FROM tv_base_data b
    INNER JOIN t2 
        ON b.platform = t2.platform 
        AND b.item_id = t2.item_id 
        AND b.pfsku_id = t2.pfsku_id 
        AND b.month_dt = t2.month_dt
    '''
    
    df_filtered = spark.sql(filter_query)
    df_filtered.createOrReplaceTempView('tv_filtered_bundle')
    
    df_no_need_process = spark.sql(f'''
        SELECT b.*
        FROM tv_base_data b
        LEFT ANTI JOIN tv_filtered_bundle f
            ON b.platform = f.platform 
            AND b.item_id = f.item_id 
            AND b.pfsku_id = f.pfsku_id 
            AND b.month_dt = f.month_dt
    ''')
    
    df_mosquito_raw = spark.sql('''
        SELECT * FROM tv_filtered_bundle
        WHERE categorys = '电热蚊香液'
        AND lower(std_spu_name) RLIKE '液|器'
    ''')
    
    if not df_mosquito_raw.rdd.isEmpty():
        from pyspark.sql.functions import udf, coalesce, lit
        merge_mosquito_udf = udf(merge_mosquito_spu_name_udf, StringType())
        
        # 为 NULL 的 std_brand_name 填充一个临时标记
        df_mosquito_raw = df_mosquito_raw.withColumn(
            'std_brand_name_join',
            coalesce(col('std_brand_name'), lit('__NULL_BRAND__'))
        )
        
        window_spec = Window.partitionBy(
            'platform', 'item_id', 'pfsku_id', 'month_dt', 'std_brand_name_join'
        ).orderBy(
            # 优先选择纯液（包含"液"但不包含"器"）作为基础记录
            when(
                lower(col('std_spu_name')).rlike('液') & 
                ~lower(col('std_spu_name')).rlike('器'), 
                1
            ).otherwise(2),
            when(col('weight').isNotNull(), 1).otherwise(2),
            col('sku_no')
        )
        
        df_mosquito_base = df_mosquito_raw.withColumn('rn', row_number().over(window_spec))
        df_mosquito_base = df_mosquito_base.filter(col('rn') == 1).drop('rn', 'categorys')
        
        from pyspark.sql.functions import struct, to_json
        df_mosquito_agg = df_mosquito_raw.groupBy(
            'platform', 'item_id', 'pfsku_id', 'month_dt', 'std_brand_name_join'
        ).agg(
            spark_min('sku_no').alias('min_sku_no'),
            # 只统计纯"液"的package_num，排除包含"器"的（如"液加热器"）
            spark_sum(
                when(
                    lower(col('std_spu_name')).rlike('液') & 
                    ~lower(col('std_spu_name')).rlike('器'),
                    col('package_num')
                ).otherwise(0)
            ).alias('total_liquid_package_num'),
            collect_list(
                to_json(struct(
                    col('sku_no').alias('sku_no'),
                    col('weight').alias('weight'),
                    col('package_num').alias('package_num'),
                    col('std_spu_name').alias('std_spu_name')
                ))
            ).alias('liquid_info_list')
        )
        
        df_mosquito_processed = df_mosquito_base.join(
            df_mosquito_agg,
            ['platform', 'item_id', 'pfsku_id', 'month_dt', 'std_brand_name_join'],
            'inner'
        ).withColumn(
            'std_spu_name',
            merge_mosquito_udf(col('liquid_info_list'))
        ).withColumn(
            'package_num',
            col('total_liquid_package_num')
        ).withColumn(
            'quantity',
            col('total_liquid_package_num')
        ).withColumn(
            'weight',
            col('weight')
        ).withColumn(
            'unit_weight',
            col('weight')
        ).drop('std_brand_name_join')
        
        window_sku = Window.partitionBy(
            'platform', 'item_id', 'pfsku_id', 'month_dt'
        ).orderBy('min_sku_no', 'std_brand_name')
        df_mosquito_processed = df_mosquito_processed.withColumn(
            'sku_no',
            row_number().over(window_sku)
        ).drop('liquid_info_list', 'total_liquid_package_num', 'min_sku_no')
    else:
        empty_schema = df_mosquito_raw.drop('categorys').schema
        df_mosquito_processed = spark.createDataFrame([], empty_schema)
    
    df_toilet_raw = spark.sql('''
        SELECT * FROM tv_filtered_bundle
        WHERE categorys = '一次性马桶刷'
    ''')
    
    if not df_toilet_raw.rdd.isEmpty():
        from pyspark.sql.functions import udf, coalesce, lit
        merge_toilet_udf = udf(merge_toilet_brush_spu_name_udf, StringType())
        
        # 为 NULL 的 std_brand_name 填充一个临时标记
        df_toilet_raw = df_toilet_raw.withColumn(
            'std_brand_name_join',
            coalesce(col('std_brand_name'), lit('__NULL_BRAND__'))
        )
        
        window_spec = Window.partitionBy(
            'platform', 'item_id', 'pfsku_id', 'month_dt', 'std_brand_name_join'
        ).orderBy(
            when(lower(col('std_spu_name')).rlike('杆|柄'), 2).otherwise(1),
            col('sku_no')
        )
        
        df_toilet_base = df_toilet_raw.withColumn('rn', row_number().over(window_spec))
        df_toilet_base = df_toilet_base.filter(col('rn') == 1).drop('rn', 'categorys')
        
        from pyspark.sql.functions import struct, to_json
        df_toilet_agg = df_toilet_raw.groupBy(
            'platform', 'item_id', 'pfsku_id', 'month_dt', 'std_brand_name_join'
        ).agg(
            spark_min('sku_no').alias('min_sku_no'),
            spark_sum(when(~lower(col('std_spu_name')).rlike('杆|柄'), col('package_num')).otherwise(0)).alias('package_num1'),
            spark_sum(when(lower(col('std_spu_name')).rlike('杆|柄'), col('package_num')).otherwise(0)).alias('package_num2'),
            collect_list(
                to_json(struct(
                    col('sku_no').alias('sku_no'),
                    col('package_num').alias('package_num'),
                    col('std_spu_name').alias('std_spu_name')
                ))
            ).alias('brush_info_list')
        )
        
        df_toilet_processed = df_toilet_base.join(
            df_toilet_agg,
            ['platform', 'item_id', 'pfsku_id', 'month_dt', 'std_brand_name_join'],
            'inner'
        ).withColumn(
            'std_spu_name',
            merge_toilet_udf(col('brush_info_list'))
        ).withColumn(
            'package_num',
            col('package_num1')
        ).withColumn(
            'quantity',
            col('package_num1')
        ).withColumn(
            'weight',
            col('weight')
        ).withColumn(
            'unit_weight',
            col('weight')
        ).drop('std_brand_name_join')
        
        window_sku = Window.partitionBy(
            'platform', 'item_id', 'pfsku_id', 'month_dt'
        ).orderBy('min_sku_no', 'std_brand_name')
        df_toilet_processed = df_toilet_processed.withColumn(
            'sku_no',
            row_number().over(window_sku)
        ).drop('brush_info_list', 'package_num1', 'package_num2', 'min_sku_no')
    else:
        empty_schema = df_toilet_raw.drop('categorys').schema
        df_toilet_processed = spark.createDataFrame([], empty_schema)
    
    df_other = spark.sql('''
        SELECT * FROM tv_filtered_bundle
        WHERE categorys NOT IN ('电热蚊香液', '一次性马桶刷')
    ''')
    
    if not df_other.rdd.isEmpty():
        # 需求1.3.1：只保留sku_no=1的那条数据，其他删除
        df_other_processed = df_other.filter(col('sku_no') == 1).drop('categorys')
    else:
        empty_schema = df_other.drop('categorys').schema
        df_other_processed = spark.createDataFrame([], empty_schema)
    
    df_processed = df_mosquito_processed.unionByName(df_toilet_processed).unionByName(df_other_processed)
    df_final_before_transform = df_no_need_process.unionByName(df_processed)
    
    rdd = df_final_before_transform.repartition(200).rdd.mapPartitions(apply_transformation)
    df = spark.createDataFrame(rdd, schema=df_final_before_transform.schema)
    
    # 统计transform前的记录数
    count_before = df.count()
    logging.info(f"Transform后记录数: {count_before}")
    
    
    # 额外保护：过滤掉主键为 NULL 的异常记录
    df = df.filter(
        col('platform').isNotNull() & 
        col('item_id').isNotNull() & 
        col('pfsku_id').isNotNull() & 
        col('month_dt').isNotNull()
    )
    
    count_after = df.count()
    logging.info(f"过滤后记录数: {count_after}, 过滤掉: {count_before - count_after} 条")
    
    return df


# ==================== 数据写入函数 ====================
def loader(spark, df: DataFrame):
    """数据写入函数"""
    # 写入前再次确认过滤 NULL 值（防止动态分区写入NULL分区）
    null_count_before_write = df.filter(col('month_dt').isNull()).count()
    if null_count_before_write > 0:
        logging.error(f"写入前发现 {null_count_before_write} 条 month_dt 为 NULL 的记录，强制过滤！")
        df = df.filter(col('month_dt').isNotNull())
    
    write_count = df.count()
    logging.info(f"准备写入 {write_count} 条记录")
    
    spark_write_hive(
        df, 
        spark, 
        TARGET_DB, 
        TARGET_TABLE, 
        create_table_ddl=DDL, 
        dynamic_partition='month_dt',
        emr=EMR,
        refresh_stats=REFRESH_STATS, 
        compression=COMPRESSION,
        repartition_num=PARTITION_NUM
    )


# ==================== 主函数 ====================
def main():
    """主函数"""
    init_logging()
    spark = SparkSession.builder.appName("onework.{{dag_name}}.{{job_name}}.{{ execution_date }}").enableHiveSupport().getOrCreate()
    spark.sql(f'set spark.sql.shuffle.partitions={PARTITION_NUM}')
    # spark.sql('set spark.sql.adaptive.enabled=true')
    # spark.sql('set spark.sql.adaptive.coalescePartitions.enabled=true')
    
    # 准备计算参数
    calc_partition = MONTH_DT if UPDATE_MODE == 'incremental' else None
    
    # 进行计算
    df = dumper(spark, calc_partition)
    loader(spark, df)
    
    spark.stop()


if __name__ == '__main__':
    exit(main())
