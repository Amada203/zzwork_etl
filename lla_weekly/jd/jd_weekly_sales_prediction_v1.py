# -*- coding: utf-8 -*-

import logging
import os
import pandas as pd
import numpy as np
import tempfile
from datetime import datetime, timedelta
from pigeon.connector import new_impala_connector, new_hive_connector
from pigeon.loader import new_csv_to_hive_loader
from pigeon.utils import init_logging

# ==================== 初始化日志 ====================
init_logging()

# ==================== 配置参数 ====================
# 数据库配置
DB = 'lla_matrank'
SOURCE_TABLE = 'jd_weekly_dwd_sku_info'  # 统一使用同一个表，通过分区区分
TARGET_TABLE = 'jd_weekly_sales_prediction'

# 使用分区表（建议）- 保留历史预测数据
USE_PARTITION_TABLE = True  # True=分区表保留历史, False=覆盖模式

# 品类过滤
CATEGORY_IDS = [
    '842', '862', '863', '12347', '12348', 
    '12350', '12352', '12417', '12597', 
    '16961', '17410', '17413', '31389', 
    '38678', '39284', '43533'
]

# 设置全局随机种子，确保可重现性
np.random.seed(42)
STANDARD_DAYS = 7

# ==================== 零评论衰减系数配置 ====================
# 根据连续0评论周数应用衰减系数
# -3: 老品漏采（无历史数据，但总评论数>=100，当前有评论）
# -2: 新商品，当前有评论（无历史数据，但当前评论数>0，且总评论数<100）
# -1: 新商品，当前无评论（无历史数据，且当前评论数=0）
# 0: 老商品，当前有评论（有历史数据，且当前评论数>0）
# 1-3: 老商品，连续1-3周0评论
# 4+: 老商品，连续4周及以上0评论（直接设为0）
ZERO_COMMENTS_DECAY_FACTORS = {
    -3: 0.9,  # 老品漏采（无历史数据，但总评论数>=100，当前有评论），轻微衰减（降低10%）
    -2: 0.8,  # 新商品，当前有评论（无历史数据，但当前评论数>0，且总评论数<100），轻微调整
    -1: 0.3,  # 新商品，当前无评论（无历史数据，且当前评论数=0），给予较低权重
    0: 1.0,   # 老商品，当前有评论（有历史数据，且当前评论数>0），正常预测，不衰减
    1: 0.5,   # 老商品，连续1周0评论，轻微衰减
    2: 0.2,   # 老商品，连续2周0评论，中等衰减
    3: 0.1,   # 老商品，连续3周0评论，大幅衰减
    4: 0.0,   # 老商品，连续4周及以上0评论，直接设为0
}

# 老品漏采判断阈值：总评论数 >= 此值认为是老品漏采
OLD_PRODUCT_MISSING_THRESHOLD = 100

# ==================== 1. 品类映射 ====================
CATEGORY_MAPPING = {
    '31389': '智能儿童手表',
    '17410': '家庭影院',
    '12347': '智能手环',
    '862': '手机耳机',
    '842': '蓝牙/无线耳机',
    '12597': '智能健康',
    '12352': '智能配饰',
    '863': '蓝牙耳机',
    '12348': '智能手表',
    '39284': '智能饰品',
    '16961': '翻译机/翻译设备',
    '17413': '回音壁/Soundbar音响',
    '38678': '智能戒指',
    '12350': '运动跟踪器',
    '43533': '运动手表',
    '12417': '多功能手表'
}

# ==================== 2. 品类特定系数 ====================
CATEGORY_COEFFICIENTS = {
    '智能儿童手表': {'base': 0.5, 'comments_weight': 1.2, 'score_weight': 0.8},
    '智能手表': {'base': 0.6, 'comments_weight': 1.0, 'score_weight': 0.9},
    '蓝牙/无线耳机': {'base': 0.7, 'comments_weight': 1.1, 'score_weight': 0.7},
    '蓝牙耳机': {'base': 0.7, 'comments_weight': 1.1, 'score_weight': 0.7},
    '智能手环': {'base': 0.5, 'comments_weight': 1.0, 'score_weight': 0.8},
    '运动手表': {'base': 0.6, 'comments_weight': 1.0, 'score_weight': 0.9},
    '运动跟踪器': {'base': 0.5, 'comments_weight': 1.0, 'score_weight': 0.8},
    '智能健康': {'base': 0.4, 'comments_weight': 0.9, 'score_weight': 0.7},
    '智能配饰': {'base': 0.3, 'comments_weight': 0.8, 'score_weight': 0.6},
    '智能饰品': {'base': 0.3, 'comments_weight': 0.8, 'score_weight': 0.6},
    '智能戒指': {'base': 0.3, 'comments_weight': 0.8, 'score_weight': 0.6},
    '多功能手表': {'base': 0.6, 'comments_weight': 1.0, 'score_weight': 0.9},
    '家庭影院': {'base': 0.3, 'comments_weight': 0.7, 'score_weight': 0.5},
    '回音壁/Soundbar音响': {'base': 0.3, 'comments_weight': 0.7, 'score_weight': 0.5},
    '手机耳机': {'base': 0.7, 'comments_weight': 1.1, 'score_weight': 0.7},
    '翻译机/翻译设备': {'base': 0.2, 'comments_weight': 0.6, 'score_weight': 0.4}
}

# ==================== 3. 工具函数 ====================
def calculate_days_diff(current_time, previous_time):
    """
    计算两个时间点的天数差（保留2位小数）
    """
    if pd.isna(current_time) or pd.isna(previous_time):
        return STANDARD_DAYS
    
    if isinstance(current_time, str):
        try:
            current_time = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S')
        except:
            return STANDARD_DAYS
    
    if isinstance(previous_time, str):
        try:
            previous_time = datetime.strptime(previous_time, '%Y-%m-%d %H:%M:%S')
        except:
            return STANDARD_DAYS
    
    time_diff = current_time - previous_time
    # 转换为天数（保留2位小数）
    # 原理：1天 = 86400秒（24小时 × 60分钟 × 60秒）
    # 秒数差 / 86400 = 天数差
    # 例如：43200秒 = 12小时 = 0.5天
    # 保留2位小数：4.3015625天 -> 4.30天，误差<0.05%
    days_diff = round(time_diff.total_seconds() / 86400.0, 2)
    
    if days_diff <= 0:
        return STANDARD_DAYS
    
    return days_diff

def normalize_to_standard_days(sales_value, data_days, standard_days=STANDARD_DAYS):
    """将销量归一化到标准天数"""
    if data_days is None or data_days <= 0:
        return sales_value
    return sales_value * standard_days / data_days

def apply_category_adjustment(predictions, category_name, comments_number):
    """应用品类特定的调整系数"""
    if category_name in CATEGORY_COEFFICIENTS:
        coeff = CATEGORY_COEFFICIENTS[category_name]
        adjustment = (coeff['base'] + 
                     np.log1p(comments_number) / 100 * coeff['comments_weight'])
        return predictions * adjustment
    return predictions

def calculate_previous_week_dt(current_next_week_dt):
    # 尝试解析日期格式
    date_format = None
    try:
        # 尝试 'YYYY-MM-DD' 格式
        dt = datetime.strptime(current_next_week_dt, '%Y-%m-%d')
        date_format = '%Y-%m-%d'
    except:
        try:
            # 尝试 'YYYYMMDD' 格式
            dt = datetime.strptime(current_next_week_dt, '%Y%m%d')
            date_format = '%Y%m%d'
        except:
            # 如果解析失败，尝试自动识别格式
            if '-' in current_next_week_dt:
                dt = datetime.strptime(current_next_week_dt, '%Y-%m-%d')
                date_format = '%Y-%m-%d'
            else:
                dt = datetime.strptime(current_next_week_dt, '%Y%m%d')
                date_format = '%Y%m%d'
    
    # 减去7天
    previous_dt = dt - timedelta(days=7)
    
    # 返回相同格式
    return previous_dt.strftime(date_format)

def calculate_previous_weeks_dt(current_next_week_dt, weeks=3):
    """
    计算前N周的next_week_dt
    
    Args:
        current_next_week_dt: 当前next_week_dt
        weeks: 往前推的周数（默认3周）
    
    Returns:
        list: [前1周, 前2周, 前3周, ...] 的next_week_dt列表
    """
    previous_weeks = []
    date_format = None
    
    # 解析日期格式
    try:
        dt = datetime.strptime(current_next_week_dt, '%Y-%m-%d')
        date_format = '%Y-%m-%d'
    except:
        try:
            dt = datetime.strptime(current_next_week_dt, '%Y%m%d')
            date_format = '%Y%m%d'
        except:
            if '-' in current_next_week_dt:
                dt = datetime.strptime(current_next_week_dt, '%Y-%m-%d')
                date_format = '%Y-%m-%d'
            else:
                dt = datetime.strptime(current_next_week_dt, '%Y%m%d')
                date_format = '%Y%m%d'
    
    # 计算前N周
    for i in range(1, weeks + 1):
        prev_dt = dt - timedelta(days=7 * i)
        previous_weeks.append(prev_dt.strftime(date_format))
    
    return previous_weeks

def calculate_consecutive_zero_weeks(comments_number, prev1_comments, prev2_comments, prev3_comments, total_comments_number=None):
    """
    计算连续0评论的周数，同时识别新商品和老品漏采
    
    Args:
        comments_number: 当前周评论数
        prev1_comments: 前1周评论数（可能为None）
        prev2_comments: 前2周评论数（可能为None）
        prev3_comments: 前3周评论数（可能为None）
        total_comments_number: 总评论数（用于判断老品漏采，可选）
    
    Returns:
        int: 
            -3: 老品漏采（无历史数据，但总评论数>=100，当前有评论）
            -2: 新商品，当前有评论（无历史数据，但当前评论数>0，且总评论数<100）
            -1: 新商品，当前无评论（无历史数据，且当前评论数=0）
            0: 老商品，当前有评论（有历史数据，且当前评论数>0）
            1-3: 老商品，连续1-3周0评论
            4: 老商品，连续4周及以上0评论
    """
    # 检查是否有历史数据
    has_history = not (prev1_comments is None or pd.isna(prev1_comments))
    
    # 如果当前周有评论
    if comments_number > 0:
        if not has_history:
            # 无历史数据，但有评论
            # 判断是否为老品漏采：总评论数 >= OLD_PRODUCT_MISSING_THRESHOLD 认为是老品漏采
            if total_comments_number is not None and total_comments_number >= OLD_PRODUCT_MISSING_THRESHOLD:
                return -3  # 老品漏采，轻微衰减（0.9）
            else:
                return -2  # 新商品，当前有评论
        else:
            return 0   # 老商品，当前有评论（不衰减）
    
    # 如果当前周无评论
    if not has_history:
        return -1  # 新商品，当前无评论
    
    # 老商品，当前无评论，检查连续0评论周数
    # 注意：此时 has_history = True，所以 prev1_comments 不是 None 也不是 NaN
    # 但 prev1_comments 可能是 0（数值0）
    if prev1_comments is not None and not pd.isna(prev1_comments) and prev1_comments > 0:
        return 1  # 连续1周0评论
    
    # prev1_comments = 0 或不存在，继续检查 prev2_comments
    if prev2_comments is None or pd.isna(prev2_comments):
        return 1  # 只有1周历史数据，且为0
    
    if prev2_comments is not None and not pd.isna(prev2_comments) and prev2_comments > 0:
        return 2  # 连续2周0评论
    
    # prev2_comments = 0，继续检查 prev3_comments
    if prev3_comments is None or pd.isna(prev3_comments):
        return 2  # 只有2周历史数据，且都为0
    
    if prev3_comments is not None and not pd.isna(prev3_comments) and prev3_comments > 0:
        return 3  # 连续3周0评论
    
    # 前3周都是0，返回4（表示4周及以上）
    return 4

def apply_zero_comments_decay(predicted_sales, consecutive_zero_weeks, comments_number):
    """
    应用连续0评论衰减系数（包含新商品调整）
    
    Args:
        predicted_sales: 原始预测销量
        consecutive_zero_weeks: 
            -3: 老品漏采
            -2: 新商品，当前有评论
            -1: 新商品，当前无评论
            0: 老商品，当前有评论
            1-4: 老商品，连续0评论周数
        comments_number: 当前总评论数（用于日志记录，不再用于判断）
    
    Returns:
        float: 调整后的预测销量
    """
    # 获取衰减系数
    decay_factor = ZERO_COMMENTS_DECAY_FACTORS.get(
        consecutive_zero_weeks, 
        0.0  # 默认：4周以上直接为0
    )
    
    # 应用衰减
    return predicted_sales * decay_factor

# ==================== 4. 核心预测函数 ====================
def fit_weekly_sales_from_reviews(comments_df, data_days_col='data_days'):
    """
    从评论数据拟合周度销量
    
    Args:
        comments_df: DataFrame，包含列：category_id, comments_number, comments_score, data_days
        
    Returns:
        result_df: DataFrame，包含预测的周度销量
    """
    result_list = []
    
    # 按品类处理
    for category_id, category_name in CATEGORY_MAPPING.items():
        category_data = comments_df[comments_df['category_id'] == category_id].copy()
        
        if len(category_data) == 0:
            continue
        
        # 安全处理评论数：两级逻辑（数据采集频率每周一次）
        # 数据现实：零值占比>90%，这是常态而非异常（数据采集频率低）
        # 逻辑优先级：
        #   1. diff > 0: 使用diff（评论增长，正常销售，符合7天销量目标）
        #   2. diff ≤ 0: 回退到total（数据采集频率低，零值是常态）
        # 说明：由于数据采集频率为每周一次，diff=0是正常业务现象，应该用total而非小比例
        if 'comments_number_diff' in category_data.columns:
            comments_diff = category_data['comments_number_diff'].values
            comments_total = category_data['comments_number'].values
            
            # diff > 0: 使用增量（符合7天销量目标）
            # diff ≤ 0: 回退到总量（数据采集频率低，零值是常态）
            safe_comments = np.where(
                comments_diff > 0,
                comments_diff,
                comments_total
            )
        else:
            # 如果没有diff列，直接用total
            safe_comments = category_data['comments_number'].values
        
        # 基础销量预测（使用safe_comments，即diff优先）
        base_sales = (
            np.log1p(safe_comments) * 10 +
            category_data['comments_score'].values * 5
        )
        
        # 应用品类调整（使用总评论数，品类系数基于历史总评论数）
        # 注意：品类调整应该基于总评论数而非增量，因为品类系数反映的是品类整体特性
        adjusted_sales = apply_category_adjustment(
            base_sales, 
            category_name, 
            category_data['comments_number'].values
        )
        
        # 归一化到7天
        if data_days_col in category_data.columns:
            data_days = category_data[data_days_col].values
            normalized_sales = np.array([
                normalize_to_standard_days(adj_sales, days) 
                for adj_sales, days in zip(adjusted_sales, data_days)
            ])
        else:
            normalized_sales = adjusted_sales
        
        # ========== 新增：零评论衰减逻辑 ==========
        # 计算连续0评论周数
        if all(col in category_data.columns for col in ['comments_number_prev1', 'comments_number_prev2', 'comments_number_prev3']):
            consecutive_zero_weeks = [
                calculate_consecutive_zero_weeks(
                    row['comments_number'],
                    row.get('comments_number_prev1'),
                    row.get('comments_number_prev2'),
                    row.get('comments_number_prev3'),
                    row.get('comments_number')  # 传入总评论数，用于判断老品漏采
                )
                for _, row in category_data.iterrows()
            ]
        elif 'comments_number_prev1' in category_data.columns:
            # 如果只有前1周数据
            consecutive_zero_weeks = [
                calculate_consecutive_zero_weeks(
                    row['comments_number'],
                    row.get('comments_number_prev1'),
                    None,
                    None,
                    row.get('comments_number')  # 传入总评论数，用于判断老品漏采
                )
                for _, row in category_data.iterrows()
            ]
        else:
            # 如果没有历史数据列，默认认为无历史数据
            # 但需要判断是否为老品漏采（总评论数 >= OLD_PRODUCT_MISSING_THRESHOLD）
            consecutive_zero_weeks = [
                calculate_consecutive_zero_weeks(
                    row['comments_number'],
                    None,
                    None,
                    None,
                    row.get('comments_number')  # 传入总评论数，用于判断老品漏采
                )
                for _, row in category_data.iterrows()
            ]
        
        # 应用零评论衰减
        normalized_sales = np.array([
            apply_zero_comments_decay(
                normalized_sales[idx],
                consecutive_zero_weeks[idx],
                row['comments_number']
            )
            for idx, (_, row) in enumerate(category_data.iterrows())
        ])
        # ========== 零评论衰减逻辑结束 ==========
        
        # 保存结果
        for idx, (_, row) in enumerate(category_data.iterrows()):
            result_row = {
                'category_id': category_id,
                'category_name': category_name,
                'comments_number': row['comments_number'],
                'comments_score': row['comments_score'],
                'predicted_weekly_sales': normalized_sales[idx],
                'data_days': row.get(data_days_col, STANDARD_DAYS)
            }
            # 添加 sku_id（如果存在）用于后续匹配
            if 'sku_id' in category_data.columns:
                result_row['sku_id'] = row['sku_id']
            result_list.append(result_row)
    
    return pd.DataFrame(result_list)

# ==================== 5. 数据查询和处理 ====================
def get_source_query(current_next_week_dt, previous_next_week_dt, previous_weeks_dt_list=None):
    """
    构建数据源查询SQL（包含历史数据）
    
    Args:
        current_next_week_dt: 当前 next_week_dt 分区值
        previous_next_week_dt: 上一周 next_week_dt 分区值（用于计算diff）
        previous_weeks_dt_list: 前N周的next_week_dt列表 [前1周, 前2周, 前3周]（用于零评论衰减）
    """
    # 构建历史数据LEFT JOIN子句
    join_clauses = []
    select_clauses = []
    
    if previous_weeks_dt_list:
        if len(previous_weeks_dt_list) > 0:
            prev1_dt = previous_weeks_dt_list[0]
            join_clauses.append(f"""
        LEFT JOIN {DB}.{SOURCE_TABLE} b1 
            ON a.product_id = b1.product_id
            AND b1.next_week_dt = '{prev1_dt}'
            """)
            select_clauses.append("IFNULL(b1.comments_number, NULL) AS comments_number_prev1")
        
        if len(previous_weeks_dt_list) > 1:
            prev2_dt = previous_weeks_dt_list[1]
            join_clauses.append(f"""
        LEFT JOIN {DB}.{SOURCE_TABLE} b2 
            ON a.product_id = b2.product_id
            AND b2.next_week_dt = '{prev2_dt}'
            """)
            select_clauses.append("IFNULL(b2.comments_number, NULL) AS comments_number_prev2")
        
        if len(previous_weeks_dt_list) > 2:
            prev3_dt = previous_weeks_dt_list[2]
            join_clauses.append(f"""
        LEFT JOIN {DB}.{SOURCE_TABLE} b3 
            ON a.product_id = b3.product_id
            AND b3.next_week_dt = '{prev3_dt}'
            """)
            select_clauses.append("IFNULL(b3.comments_number, NULL) AS comments_number_prev3")
    
    # 构建SELECT子句（历史数据列）
    history_select = ', '.join(select_clauses) if select_clauses else ''
    if history_select:
        history_select = ', ' + history_select
    
    sql = f"""
    SELECT 
        a.sku_id,
        a.product_id,
        a.category_id,
        a.category_name,
        a.comments_number,
        a.comments_score,
        a.original_price,
        a.purchase_price,
        a.dt,
        a.next_week_dt,
        a.comments_number - IFNULL(b.comments_number, 0) AS comments_number_diff,
        ROUND(CAST((UNIX_TIMESTAMP(a.snapshot_time, 'yyyy-MM-dd HH:mm:ss') - 
                    UNIX_TIMESTAMP(IFNULL(b.snapshot_time, a.snapshot_time), 'yyyy-MM-dd HH:mm:ss')) AS DOUBLE) / 86400.0, 2) AS days_diff{history_select}
    FROM {DB}.{SOURCE_TABLE} a
    LEFT JOIN {DB}.{SOURCE_TABLE} b 
        ON a.product_id = b.product_id
        AND b.next_week_dt = '{previous_next_week_dt}'
    {''.join(join_clauses)}
    WHERE a.next_week_dt = '{current_next_week_dt}'
        AND a.category_id IN ({','.join([f"'{cat}'" for cat in CATEGORY_IDS])})
    """
    return sql

def get_current_next_week_dt(impala):
    """
    从源表中获取最新的 next_week_dt 分区值
    
    Returns:
        current_next_week_dt: 当前最新的 next_week_dt 分区值，字符串格式
    """
    sql = f"""
    SELECT MAX(next_week_dt) as max_next_week_dt
    FROM {DB}.{SOURCE_TABLE}
    """
    rows = impala.fetchall(sql)
    if not rows or not rows[0][0]:
        raise ValueError(f"无法从 {DB}.{SOURCE_TABLE} 获取最新的 next_week_dt 分区值")
    
    current_next_week_dt = rows[0][0]
    logging.info(f"✓ 获取到当前 next_week_dt: {current_next_week_dt}")
    return current_next_week_dt

def create_target_table(impala, use_partition):
    """创建目标表"""
    if use_partition:
        # 分区表模式：使用 next_week_dt 作为分区字段
        ddl = f"""
        CREATE TABLE {DB}.{TARGET_TABLE} (
            sku_id STRING COMMENT 'SKU ID',
            category_id STRING COMMENT '品类ID',
            category_name STRING COMMENT '品类名称',
            comments_number BIGINT COMMENT '总评论数',
            comments_score DOUBLE COMMENT '综合评分',
            predicted_weekly_sales BIGINT COMMENT '预测的周度销量（归一化到7天）',
            comments_number_diff BIGINT COMMENT '评论数增量（差值）',
            days_diff DOUBLE COMMENT '数据时间差（天数）',
            dt STRING COMMENT '日期',
            prediction_dt STRING COMMENT '预测日期（更新时间）'
        )
        PARTITIONED BY (next_week_dt STRING COMMENT '下周一分区（业务分区）')
        STORED AS PARQUET
        """
        
        # 分区表模式：只创建表（如果不存在），保留历史数据
        logging.info(f"检查分区表是否存在: {DB}.{TARGET_TABLE}")
        try:
            impala.execute(f"DESCRIBE {DB}.{TARGET_TABLE}")
            logging.info(f"✓ 分区表已存在，保留历史数据")
        except:
            logging.info(f"分区表不存在，创建新表")
            impala.execute(ddl)
    else:
        # 覆盖模式：普通表
        ddl = f"""
        CREATE TABLE {DB}.{TARGET_TABLE} (
            sku_id STRING COMMENT 'SKU ID',
            category_id STRING COMMENT '品类ID',
            category_name STRING COMMENT '品类名称',
            comments_number BIGINT COMMENT '总评论数',
            comments_score DOUBLE COMMENT '综合评分',
            predicted_weekly_sales BIGINT COMMENT '预测的周度销量（归一化到7天）',
            comments_number_diff BIGINT COMMENT '评论数增量（差值）',
            days_diff DOUBLE COMMENT '数据时间差（天数）',
            dt STRING COMMENT '日期',
            prediction_dt STRING COMMENT '预测日期（更新时间）',
            next_week_dt STRING COMMENT '下周一'
        )
        STORED AS PARQUET
        """
        
        # 覆盖模式：删除并重建表
        logging.info(f"覆盖模式：删除并重建表: {DB}.{TARGET_TABLE}")
        try:
            impala.execute(f"DROP TABLE IF EXISTS {DB}.{TARGET_TABLE}")
            logging.info(f"✓ 旧表已删除")
        except Exception as e:
            logging.warning(f"删除旧表时出错: {e}")
        
        impala.execute(ddl)
        logging.info(f"✓ 普通表已创建")

# ==================== 6. 主处理流程 ====================
def main():
    """主处理流程"""
    impala = None
    hive = None
    temp_table = None
    csv_path = None
    merged_temp = None  # 用于存储文件合并的临时表名
    
    try:
        # 1. 初始化连接
        logging.info("初始化Impala连接...")
        impala = new_impala_connector(emr=True)
        logging.info("✓ 连接成功")
        
        # 2. 创建目标表
        create_target_table(impala, USE_PARTITION_TABLE)
        
        # 获取当前预测日期
        current_date = datetime.now().strftime('%Y%m%d')
        
        # 3. 获取当前和上一周的 next_week_dt
        logging.info("获取当前 next_week_dt 分区值...")
        current_next_week_dt = get_current_next_week_dt(impala)
        previous_next_week_dt = calculate_previous_week_dt(current_next_week_dt)
        
        # 计算前3周的next_week_dt（用于零评论衰减逻辑）
        previous_weeks_dt_list = calculate_previous_weeks_dt(current_next_week_dt, weeks=3)
        logging.info(f"✓ 当前 next_week_dt: {current_next_week_dt}")
        logging.info(f"✓ 上一周 next_week_dt: {previous_next_week_dt}")
        logging.info(f"✓ 历史周数: {previous_weeks_dt_list}")
        
        # 4. 读取数据（包含历史数据）
        logging.info("从Impala读取数据（包含历史3周数据）...")
        sql = get_source_query(current_next_week_dt, previous_next_week_dt, previous_weeks_dt_list)
        rows = impala.fetchall(sql)
        logging.info(f"✓ 成功读取 {len(rows)} 条记录")
        
        if len(rows) == 0:
            logging.warning("没有数据需要处理")
            return
        
        # 5. 转换为DataFrame
        # 注意：SQL查询包含product_id，但只在join时使用，不输出到最终表
        columns = [
            'sku_id', 'product_id', 'category_id', 'category_name', 
            'comments_number', 'comments_score', 'original_price', 'purchase_price',
            'dt', 'next_week_dt', 'comments_number_diff', 'days_diff'
        ]
        
        # 添加历史数据列（如果存在）
        if previous_weeks_dt_list:
            if len(previous_weeks_dt_list) > 0:
                columns.append('comments_number_prev1')
            if len(previous_weeks_dt_list) > 1:
                columns.append('comments_number_prev2')
            if len(previous_weeks_dt_list) > 2:
                columns.append('comments_number_prev3')
        
        df = pd.DataFrame(rows, columns=columns)
        # 删除product_id列（与sku_id重复）
        if 'product_id' in df.columns:
            df = df.drop(columns=['product_id'])
        logging.info(f"✓ DataFrame创建成功，Shape: {df.shape}")
        
        # 6. 执行预测
        logging.info("执行销量预测...")
        
        # 数据验证：检查评论数增量
        neg_count = (df['comments_number_diff'] < 0).sum()
        zero_count = (df['comments_number_diff'] == 0).sum()
        pos_count = (df['comments_number_diff'] > 0).sum()
        total_count = len(df)
        
        if neg_count > 0 or zero_count > 0:
            logging.warning(f"⚠️ 评论数差值统计：")
            logging.warning(f"   正数: {pos_count}条 ({pos_count/total_count*100:.2f}%)")
            logging.warning(f"   零值: {zero_count}条 ({zero_count/total_count*100:.2f}%)")
            logging.warning(f"   负数: {neg_count}条 ({neg_count/total_count*100:.2f}%)")
            
            if zero_count / total_count > 0.5:
                logging.warning(f"   ⚠️ 零值占比超过50%，说明大部分商品本周无新增评论（数据采集频率每周一次）")
            
            logging.info(f"   预测策略（两级逻辑，数据采集频率每周一次）：")
            logging.info(f"     1. diff>0: 使用差值（评论增长，正常销售，符合7天销量目标）")
            logging.info(f"     2. diff≤0: 回退到总评论数（数据采集频率低，零值是常态）")
        
        result_df = fit_weekly_sales_from_reviews(df, data_days_col='days_diff')
        logging.info(f"✓ 预测完成，生成 {len(result_df)} 条结果")
        
        # 7. 准备输出数据
        logging.info("准备写入数据...")
        
        # 关键：确保 result_df 和 df 的行一一对应
        # 由于 fit_weekly_sales_from_reviews 按品类处理，result_df 的顺序与 df 不同
        # 方法：使用 sku_id 从 df 中提取对应行的其他字段
        
        # 从 result_df 中提取预测相关的列（不包括 comments_number，因为要使用总评论数）
        prediction_cols = ['sku_id', 'category_id', 'category_name', 
                          'comments_score', 'predicted_weekly_sales']
        output_df = result_df[prediction_cols].copy()
        
        # 将 predicted_weekly_sales 转换为整数（BIGINT）
        # 处理 NaN 和 inf 值，替换为 0
        before_fill = output_df['predicted_weekly_sales'].isna().sum() + (output_df['predicted_weekly_sales'] == np.inf).sum() + (output_df['predicted_weekly_sales'] == -np.inf).sum()
        
        output_df['predicted_weekly_sales'] = output_df['predicted_weekly_sales'].replace([np.inf, -np.inf], np.nan).fillna(0).round().astype(int)
        
        if before_fill > 0:
            logging.warning(f"⚠️ 预测销量异常值统计：NaN/Inf 共 {before_fill} 条已替换为 0")
        
        # 从 df 中添加字段（基于 (sku_id, category_id) 组合键匹配）
        # 特别注意：comments_number 使用总评论数，而不是被修改后的值
        merge_fields = ['comments_number', 'comments_number_diff', 'days_diff', 'dt', 'next_week_dt']
        df['match_key'] = df['sku_id'] + '_' + df['category_id'].astype(str)
        output_df['match_key'] = output_df['sku_id'] + '_' + output_df['category_id'].astype(str)
        
        for field in merge_fields:
            if field in df.columns:
                # 创建映射字典：(sku_id, category_id) -> field value
                field_map = dict(zip(df['match_key'], df[field]))
                # 使用组合键从 df 中查找对应的值
                output_df[field] = output_df['match_key'].map(field_map)
                # 检查是否有缺失值并处理
                if output_df[field].isna().any():
                    missing_count = output_df[field].isna().sum()
                    # 数值型字段（BIGINT/DOUBLE）填充为 0，字符串型填充为空字符串
                    if field in ['comments_number', 'comments_number_diff', 'days_diff']:
                        output_df[field] = output_df[field].fillna(0)
                        logging.warning(f"⚠️ 警告：{field} 字段有 {missing_count} 个缺失值，已填充为 0")
                    else:
                        output_df[field] = output_df[field].fillna('')
                        logging.warning(f"⚠️ 警告：{field} 字段有 {missing_count} 个缺失值，已填充为空字符串")
        
        # 添加 prediction_dt 字段（预测日期）
        output_df['prediction_dt'] = current_date
        
        # 删除临时匹配键
        output_df = output_df.drop(columns=['match_key'])
        
        # 确保列顺序并转换数据类型（comments_number 现在是总评论数）
        # 将 BIGINT 类型字段转换为整数
        bigint_fields = ['comments_number', 'comments_number_diff', 'predicted_weekly_sales']
        for field in bigint_fields:
            if field in output_df.columns:
                output_df[field] = output_df[field].round().astype(int)
        
        if USE_PARTITION_TABLE:
            # 分区表的列顺序（next_week_dt 作为分区字段在最后）
            output_cols = [
                'sku_id', 'category_id', 'category_name',
                'comments_number', 'comments_score', 'predicted_weekly_sales',
                'comments_number_diff', 'days_diff', 'dt', 'prediction_dt', 'next_week_dt'
            ]
        else:
            # 普通表的列顺序（包含 next_week_dt）
            output_cols = [
                'sku_id', 'category_id', 'category_name',
                'comments_number', 'comments_score', 'predicted_weekly_sales',
                'comments_number_diff', 'days_diff', 'dt', 'prediction_dt', 'next_week_dt'
            ]
        
        output_df = output_df[output_cols]
        
        # 8. 写入临时CSV
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            csv_path = f.name
            output_df.to_csv(csv_path, index=False, header=False)
        logging.info(f"✓ 临时CSV已保存")
        
        # 9. 加载数据到Impala
        temp_table = f'{TARGET_TABLE}_temp_{int(datetime.now().timestamp())}'
        logging.info(f"创建临时表: {DB}.{temp_table}")
        
        impala.execute(f"DROP TABLE IF EXISTS {DB}.{temp_table}")
        
        # 临时表使用普通表结构（不分区），便于加载CSV数据
        if USE_PARTITION_TABLE:
            # 分区表模式：创建普通表
            # next_week_dt 作为普通字段包含在临时表中，插入时作为分区值使用
            temp_ddl = f"""
            CREATE TABLE {DB}.{temp_table} (
                sku_id STRING,
                category_id STRING,
                category_name STRING,
                comments_number BIGINT,
                comments_score DOUBLE,
                predicted_weekly_sales BIGINT,
                comments_number_diff BIGINT,
                days_diff DOUBLE,
                dt STRING,
                prediction_dt STRING,
                next_week_dt STRING
            )
            STORED AS PARQUET
            """
            impala.execute(temp_ddl)
        else:
            # 覆盖模式：继承目标表结构
            impala.execute(f"CREATE TABLE {DB}.{temp_table} LIKE {DB}.{TARGET_TABLE}")
        
        if hive is None:
            hive = new_hive_connector(emr=True)
        loader = new_csv_to_hive_loader(
            table=temp_table,
            filename=csv_path,
            database=DB,
            hive_connector=hive,
            impala_connector=impala,
            is_std_csv=True,
            delete_file=True,
            has_header=False
        )
        loader.execute()
        logging.info(f"✓ 数据加载完成")
        
        # 10. 导入目标表
        logging.info("导入数据到目标表...")
        if USE_PARTITION_TABLE:
            # 分区表模式：只更新当前分区，保留其他分区历史数据
            logging.info(f"使用分区表模式")
            
            # 验证临时表中只有一个分区值（安全校验）
            partition_check_sql = f"SELECT DISTINCT next_week_dt FROM {DB}.{temp_table}"
            partition_rows = impala.fetchall(partition_check_sql)
            if not partition_rows:
                raise ValueError(f"临时表 {DB}.{temp_table} 中没有数据")
            
            partition_values = [row[0] for row in partition_rows]
            if len(partition_values) > 1:
                raise ValueError(f"⚠️ 警告：临时表中有多个分区值: {partition_values}，这可能导致意外写入多个分区")
            
            partition_value = partition_values[0]
            logging.info(f"当前更新分区: next_week_dt={partition_value}")
            
            # 验证分区值是否与预期的当前分区一致（可选校验）
            if partition_value != current_next_week_dt:
                logging.warning(f"⚠️ 注意：临时表中的分区值 ({partition_value}) 与查询时使用的当前分区 ({current_next_week_dt}) 不一致")
            
            # 先删除该分区（如果存在）
            try:
                impala.execute(f"ALTER TABLE {DB}.{TARGET_TABLE} DROP IF EXISTS PARTITION (next_week_dt='{partition_value}')")
                logging.info(f"✓ 已删除旧分区（如果存在）")
            except Exception as e:
                logging.warning(f"删除分区时出错: {e}")
            
            # 使用 INSERT OVERWRITE 只覆盖当前分区，不影响其他分区
            insert_sql = f"""
            INSERT OVERWRITE TABLE {DB}.{TARGET_TABLE} PARTITION (next_week_dt)
            SELECT 
                sku_id,
                category_id,
                category_name,
                comments_number,
                comments_score,
                predicted_weekly_sales,
                comments_number_diff,
                days_diff,
                dt,
                prediction_dt,
                next_week_dt
            FROM {DB}.{temp_table}
            """
            impala.execute(insert_sql)
            logging.info(f"✓ 数据已导入到分区，其他分区历史数据已保留")
        else:
            # 覆盖模式：替换全部数据
            impala.execute(f"INSERT OVERWRITE TABLE {DB}.{TARGET_TABLE} SELECT * FROM {DB}.{temp_table}")
            logging.info(f"✓ 数据已导入（覆盖模式）")
        
        # 10.1. 合并小文件（优化存储）
        logging.info("合并小文件优化存储...")
        try:
            check_sql = f"SHOW FILES IN {DB}.{TARGET_TABLE}"
            files_before = impala.fetchall(check_sql)
            file_count = len(files_before) if files_before else 0
            logging.info(f"目标表现有文件数: {file_count}")
            
            if file_count > 5:
                merged_temp = f'{TARGET_TABLE}_merged_temp_{int(datetime.now().timestamp())}'
                impala.execute(f"DROP TABLE IF EXISTS {DB}.{merged_temp}")
                impala.execute(f"CREATE TABLE {DB}.{merged_temp} LIKE {DB}.{TARGET_TABLE}")
                
                if USE_PARTITION_TABLE:
                    impala.execute(f"""
                        INSERT INTO {DB}.{merged_temp} PARTITION (next_week_dt)
                        SELECT sku_id, category_id, category_name, comments_number, comments_score,
                               predicted_weekly_sales, comments_number_diff, days_diff, dt, prediction_dt, next_week_dt
                        FROM {DB}.{TARGET_TABLE}
                    """)
                else:
                    impala.execute(f"INSERT INTO {DB}.{merged_temp} SELECT * FROM {DB}.{TARGET_TABLE}")
                
                impala.execute(f"DROP TABLE IF EXISTS {DB}.{TARGET_TABLE}")
                impala.execute(f"ALTER TABLE {DB}.{merged_temp} RENAME TO {TARGET_TABLE}")
                merged_temp = None  # 标记已重命名，无需清理
                
                files_after = impala.fetchall(check_sql)
                file_count_after = len(files_after) if files_after else 0
                logging.info(f"✓ 文件合并完成，文件数: {file_count} -> {file_count_after}")
            else:
                logging.info(f"✓ 文件数较少，无需合并")
        except Exception as e:
            logging.warning(f"合并小文件失败（不影响结果）: {e}")
        
        # 11. 清理临时表
        impala.execute(f"DROP TABLE IF EXISTS {DB}.{temp_table}")
        logging.info(f"✓ 临时表已删除")
        
        # 12. 删除临时CSV文件（如果loader没有自动删除）
        try:
            if csv_path and os.path.exists(csv_path):
                os.remove(csv_path)
                logging.info(f"✓ 临时CSV文件已删除")
        except Exception as e:
            logging.warning(f"删除临时CSV文件失败: {e}")
        
        # 13. 统计信息
        stats_sql = f"""
        SELECT 
            COUNT(*) as total_count,
            AVG(predicted_weekly_sales) as avg_predicted_sales,
            MIN(predicted_weekly_sales) as min_predicted_sales,
            MAX(predicted_weekly_sales) as max_predicted_sales
        FROM {DB}.{TARGET_TABLE}
        """
        stats = impala.fetchall(stats_sql)[0]
        logging.info("\n" + "=" * 60)
        logging.info("📊 预测结果统计")
        logging.info("=" * 60)
        logging.info(f"总记录数: {stats[0]:,}")
        logging.info(f"平均预测销量: {stats[1]:.2f}")
        logging.info(f"最小预测销量: {stats[2]:.2f}")
        logging.info(f"最大预测销量: {stats[3]:.2f}")
        logging.info("=" * 60)
        
        logging.info("\n✅ 处理完成！")
        logging.info(f"结果表: {DB}.{TARGET_TABLE}")
        
    except Exception as e:
        logging.error(f"❌ 处理失败: {str(e)}", exc_info=True)
        raise
        
    finally:
        # 清理临时资源
        try:
            if impala:
                if temp_table:
                    logging.info("清理临时表资源...")
                    impala.execute(f"DROP TABLE IF EXISTS {DB}.{temp_table}")
                    logging.info(f"✓ 临时表已清理: {temp_table}")
                
                # 清理 merged_temp（如果存在且未被重命名）
                if merged_temp:
                    logging.info("清理文件合并临时表...")
                    impala.execute(f"DROP TABLE IF EXISTS {DB}.{merged_temp}")
                    logging.info(f"✓ 文件合并临时表已清理: {merged_temp}")
        except Exception as e:
            logging.warning(f"清理临时表失败: {e}")
        
        try:
            if csv_path and os.path.exists(csv_path):
                os.remove(csv_path)
                logging.info(f"✓ 临时CSV已清理")
        except Exception as e:
            logging.warning(f"清理临时CSV失败: {e}")

if __name__ == '__main__':
    logging.info("=" * 60)
    logging.info("JD周度销量预测 - 开始执行 (V2版本)")
    logging.info("=" * 60)
    
    main()

