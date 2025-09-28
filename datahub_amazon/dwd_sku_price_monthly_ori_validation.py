# -*- coding: utf-8 -*-
"""
dwd_sku_price_monthly_ori 表数据验证脚本
验证月度SKU价格表的数据质量和字段映射逻辑
根据 mapping 文档独立实现验证逻辑，确保业务规则正确性
"""

import logging
import pandas as pd
from pigeon.utils import init_logging
from pigeon.connector import new_impala_connector

# ==================== 配置参数 ====================
DATABASE = 'datahub_amazon'
TABLE = 'dwd_sku_price_monthly_ori'
UPSTREAM_TABLE = 'datahub_amazon.dwd_sku_price_daily'
TEST_MONTH = '2025-08-01'  # 测试月份
SAMPLE_LIMIT = 10  # 样本数量限制

def validate_data_quality():
    """
    验证数据质量
    """
    print(f"\n=== 📊 数据质量验证 ===")
    
    try:
        impala = new_impala_connector(emr=True)
        
        # 检查数据存在性
        count_query = f"""
        SELECT COUNT(*) as total_count
        FROM {DATABASE}.{TABLE}
        WHERE month_dt = '{TEST_MONTH}'
        """
        
        count_result = impala.get_pandas_df(count_query)
        total_count = count_result.iloc[0]['total_count'] if len(count_result) > 0 else 0
        
        if total_count > 0:
            print(f"✅ 数据存在，{TABLE}记录数: {total_count}条")
        else:
            print(f"❌ 数据不存在，{TABLE}记录数: {total_count}条")
            return False
            
    except Exception as e:
        print(f"❌ 数据质量验证失败: {e}")
        return False
    
    return True

def validate_primary_key():
    """
    验证主键唯一性：sku_id + region + month_dt
    """
    print(f"\n=== 🔑 主键唯一性验证 ===")
    
    try:
        impala = new_impala_connector(emr=True)
        
        # 检查主键唯一性
        primary_key_query = f"""
        SELECT 
            sku_id,
            region,
            month_dt,
            COUNT(*) as duplicate_count
        FROM {DATABASE}.{TABLE}
        WHERE month_dt = '{TEST_MONTH}'
        GROUP BY sku_id, region, month_dt
        HAVING COUNT(*) > 1
        LIMIT 10
        """
        
        duplicate_result = impala.get_pandas_df(primary_key_query)
        
        if len(duplicate_result) == 0:
            print("✅ 主键唯一性验证通过：sku_id + region + month_dt 组合唯一")
        else:
            print("❌ 主键唯一性验证失败：存在重复记录")
            print("重复记录示例：")
            print(duplicate_result.to_string())
            return False
            
    except Exception as e:
        print(f"❌ 主键唯一性验证失败: {e}")
        return False
    
    return True

def validate_price_aggregation_logic():
    """
    验证价格聚合逻辑：min、max、avg计算是否正确
    """
    print(f"\n=== 💰 价格聚合逻辑验证 ===")
    
    try:
        impala = new_impala_connector(emr=True)
        
        # 验证价格聚合逻辑
        price_agg_query = f"""
        WITH upstream_aggregation AS (
            SELECT 
                sku_id,
                region,
                date_format(dt, 'yyyy-MM-01') as month_dt,
                MIN(CASE 
                    WHEN sku_list_price IS NOT NULL AND sku_list_price != '' 
                         AND CAST(sku_list_price AS DOUBLE) > 0
                    THEN CAST(sku_list_price AS DOUBLE) 
                    ELSE NULL 
                END) as upstream_list_min,
                MAX(CASE 
                    WHEN sku_list_price IS NOT NULL AND sku_list_price != '' 
                         AND CAST(sku_list_price AS DOUBLE) > 0
                    THEN CAST(sku_list_price AS DOUBLE) 
                    ELSE NULL 
                END) as upstream_list_max,
                AVG(CASE 
                    WHEN sku_list_price IS NOT NULL AND sku_list_price != '' 
                         AND CAST(sku_list_price AS DOUBLE) > 0
                    THEN CAST(sku_list_price AS DOUBLE) 
                    ELSE NULL 
                END) as upstream_list_avg,
                MIN(CASE 
                    WHEN sku_online_price IS NOT NULL AND sku_online_price != '' 
                         AND CAST(sku_online_price AS DOUBLE) > 0
                    THEN CAST(sku_online_price AS DOUBLE) 
                    ELSE NULL 
                END) as upstream_online_min,
                MAX(CASE 
                    WHEN sku_online_price IS NOT NULL AND sku_online_price != '' 
                         AND CAST(sku_online_price AS DOUBLE) > 0
                    THEN CAST(sku_online_price AS DOUBLE) 
                    ELSE NULL 
                END) as upstream_online_max,
                AVG(CASE 
                    WHEN sku_online_price IS NOT NULL AND sku_online_price != '' 
                         AND CAST(sku_online_price AS DOUBLE) > 0
                    THEN CAST(sku_online_price AS DOUBLE) 
                    ELSE NULL 
                END) as upstream_online_avg
            FROM {UPSTREAM_TABLE}
            WHERE date_format(dt, 'yyyy-MM-01') = '{TEST_MONTH}'
              AND region IS NOT NULL AND region != ''
              AND sku_id IS NOT NULL AND sku_id != ''
              AND (sku_online_price IS NOT NULL OR sku_list_price IS NOT NULL)
            GROUP BY sku_id, region, date_format(dt, 'yyyy-MM-01')
        ),
        target_aggregation AS (
            SELECT 
                sku_id,
                region,
                month_dt,
                sku_list_price_min,
                sku_list_price_max,
                sku_list_price_avg,
                sku_online_price_min,
                sku_online_price_max,
                sku_online_price_avg
            FROM {DATABASE}.{TABLE}
            WHERE month_dt = '{TEST_MONTH}'
        )
        SELECT 
            t.sku_id,
            t.region,
            t.sku_list_price_min as target_list_min,
            u.upstream_list_min,
            t.sku_list_price_max as target_list_max,
            u.upstream_list_max,
            t.sku_list_price_avg as target_list_avg,
            u.upstream_list_avg,
            t.sku_online_price_min as target_online_min,
            u.upstream_online_min,
            t.sku_online_price_max as target_online_max,
            u.upstream_online_max,
            t.sku_online_price_avg as target_online_avg,
            u.upstream_online_avg,
            CASE 
                WHEN ABS(t.sku_list_price_min - u.upstream_list_min) < 0.001 THEN 'PASS'
                WHEN t.sku_list_price_min IS NULL AND u.upstream_list_min IS NULL THEN 'PASS'
                ELSE 'FAIL'
            END as list_min_match,
            CASE 
                WHEN ABS(t.sku_list_price_max - u.upstream_list_max) < 0.001 THEN 'PASS'
                WHEN t.sku_list_price_max IS NULL AND u.upstream_list_max IS NULL THEN 'PASS'
                ELSE 'FAIL'
            END as list_max_match,
            CASE 
                WHEN ABS(t.sku_list_price_avg - u.upstream_list_avg) < 0.001 THEN 'PASS'
                WHEN t.sku_list_price_avg IS NULL AND u.upstream_list_avg IS NULL THEN 'PASS'
                ELSE 'FAIL'
            END as list_avg_match,
            CASE 
                WHEN ABS(t.sku_online_price_min - u.upstream_online_min) < 0.001 THEN 'PASS'
                WHEN t.sku_online_price_min IS NULL AND u.upstream_online_min IS NULL THEN 'PASS'
                ELSE 'FAIL'
            END as online_min_match,
            CASE 
                WHEN ABS(t.sku_online_price_max - u.upstream_online_max) < 0.001 THEN 'PASS'
                WHEN t.sku_online_price_max IS NULL AND u.upstream_online_max IS NULL THEN 'PASS'
                ELSE 'FAIL'
            END as online_max_match,
            CASE 
                WHEN ABS(t.sku_online_price_avg - u.upstream_online_avg) < 0.001 THEN 'PASS'
                WHEN t.sku_online_price_avg IS NULL AND u.upstream_online_avg IS NULL THEN 'PASS'
                ELSE 'FAIL'
            END as online_avg_match
        FROM target_aggregation t
        LEFT JOIN upstream_aggregation u ON (
            t.sku_id = u.sku_id AND 
            t.region = u.region AND 
            t.month_dt = u.month_dt
        )
        WHERE (ABS(t.sku_list_price_min - u.upstream_list_min) >= 0.001 
               OR ABS(t.sku_list_price_max - u.upstream_list_max) >= 0.001
               OR ABS(t.sku_list_price_avg - u.upstream_list_avg) >= 0.001
               OR ABS(t.sku_online_price_min - u.upstream_online_min) >= 0.001
               OR ABS(t.sku_online_price_max - u.upstream_online_max) >= 0.001
               OR ABS(t.sku_online_price_avg - u.upstream_online_avg) >= 0.001)
        LIMIT {SAMPLE_LIMIT}
        """
        
        mismatch_result = impala.get_pandas_df(price_agg_query)
        
        if len(mismatch_result) == 0:
            print("✅ 价格聚合逻辑验证通过：所有价格统计都正确")
        else:
            print("❌ 价格聚合逻辑验证失败：存在价格统计不匹配的记录")
            print("不匹配记录示例：")
            print(mismatch_result.to_string())
            return False
            
    except Exception as e:
        print(f"❌ 价格聚合逻辑验证失败: {e}")
        return False
    
    return True

def validate_default_price_logic():
    """
    验证默认价格逻辑：sku_list_price取sku_list_price_min，sku_online_price取sku_online_price_min
    """
    print(f"\n=== 🎯 默认价格逻辑验证 ===")
    
    try:
        impala = new_impala_connector(emr=True)
        
        # 验证默认价格逻辑
        default_price_query = f"""
        SELECT 
            sku_id,
            region,
            sku_list_price,
            sku_list_price_min,
            sku_online_price,
            sku_online_price_min,
            CASE 
                WHEN ABS(sku_list_price - sku_list_price_min) < 0.001 THEN 'PASS'
                WHEN sku_list_price IS NULL AND sku_list_price_min IS NULL THEN 'PASS'
                ELSE 'FAIL'
            END as list_price_default_check,
            CASE 
                WHEN ABS(sku_online_price - sku_online_price_min) < 0.001 THEN 'PASS'
                WHEN sku_online_price IS NULL AND sku_online_price_min IS NULL THEN 'PASS'
                ELSE 'FAIL'
            END as online_price_default_check
        FROM {DATABASE}.{TABLE}
        WHERE month_dt = '{TEST_MONTH}'
          AND (ABS(sku_list_price - sku_list_price_min) >= 0.001 
               OR ABS(sku_online_price - sku_online_price_min) >= 0.001)
        LIMIT {SAMPLE_LIMIT}
        """
        
        mismatch_result = impala.get_pandas_df(default_price_query)
        
        if len(mismatch_result) == 0:
            print("✅ 默认价格逻辑验证通过：默认价格字段取值正确")
        else:
            print("❌ 默认价格逻辑验证失败：存在默认价格不匹配的记录")
            print("不匹配记录示例：")
            print(mismatch_result.to_string())
            return False
            
    except Exception as e:
        print(f"❌ 默认价格逻辑验证失败: {e}")
        return False
    
    return True

def validate_midrange_fields():
    """
    验证中位数字段：sku_list_price_midrange和sku_online_price_midrange应该为NULL
    """
    print(f"\n=== 📊 中位数字段验证 ===")
    
    try:
        impala = new_impala_connector(emr=True)
        
        # 验证中位数字段
        midrange_query = f"""
        SELECT 
            sku_id,
            region,
            sku_list_price_midrange,
            sku_online_price_midrange,
            CASE 
                WHEN sku_list_price_midrange IS NULL THEN 'PASS'
                ELSE 'FAIL'
            END as list_midrange_check,
            CASE 
                WHEN sku_online_price_midrange IS NULL THEN 'PASS'
                ELSE 'FAIL'
            END as online_midrange_check
        FROM {DATABASE}.{TABLE}
        WHERE month_dt = '{TEST_MONTH}'
          AND (sku_list_price_midrange IS NOT NULL OR sku_online_price_midrange IS NOT NULL)
        LIMIT {SAMPLE_LIMIT}
        """
        
        invalid_result = impala.get_pandas_df(midrange_query)
        
        if len(invalid_result) == 0:
            print("✅ 中位数字段验证通过：所有中位数字段都为NULL")
        else:
            print("❌ 中位数字段验证失败：存在非NULL的中位数字段")
            print("错误记录示例：")
            print(invalid_result.to_string())
            return False
            
    except Exception as e:
        print(f"❌ 中位数字段验证失败: {e}")
        return False
    
    return True

def validate_mode_calculation():
    """
    验证众数计算逻辑：取出现次数最多价格，平局时取最低价
    """
    print(f"\n=== 🔢 众数计算逻辑验证 ===")
    
    try:
        impala = new_impala_connector(emr=True)
        
        # 验证众数计算逻辑
        mode_query = f"""
        WITH upstream_mode AS (
            SELECT 
                sku_id,
                region,
                date_format(dt, 'yyyy-MM-01') as month_dt,
                sku_list_price,
                sku_online_price,
                COUNT(*) as list_price_count,
                COUNT(*) as online_price_count,
                ROW_NUMBER() OVER (
                    PARTITION BY sku_id, region, date_format(dt, 'yyyy-MM-01')
                    ORDER BY COUNT(sku_list_price) DESC, sku_list_price ASC
                ) as list_price_mode_rn,
                ROW_NUMBER() OVER (
                    PARTITION BY sku_id, region, date_format(dt, 'yyyy-MM-01')
                    ORDER BY COUNT(sku_online_price) DESC, sku_online_price ASC
                ) as online_price_mode_rn
            FROM {UPSTREAM_TABLE}
            WHERE date_format(dt, 'yyyy-MM-01') = '{TEST_MONTH}'
              AND region IS NOT NULL AND region != ''
              AND sku_id IS NOT NULL AND sku_id != ''
              AND (sku_online_price IS NOT NULL OR sku_list_price IS NOT NULL)
            GROUP BY sku_id, region, date_format(dt, 'yyyy-MM-01'), sku_list_price, sku_online_price
        ),
        expected_mode AS (
            SELECT 
                sku_id,
                region,
                month_dt,
                MAX(CASE WHEN list_price_mode_rn = 1 THEN sku_list_price END) as expected_list_mode,
                MAX(CASE WHEN online_price_mode_rn = 1 THEN sku_online_price END) as expected_online_mode
            FROM upstream_mode
            GROUP BY sku_id, region, month_dt
        ),
        target_mode AS (
            SELECT 
                sku_id,
                region,
                month_dt,
                sku_list_price_mode,
                sku_online_price_mode
            FROM {DATABASE}.{TABLE}
            WHERE month_dt = '{TEST_MONTH}'
        )
        SELECT 
            t.sku_id,
            t.region,
            t.sku_list_price_mode as target_list_mode,
            e.expected_list_mode,
            t.sku_online_price_mode as target_online_mode,
            e.expected_online_mode,
            CASE 
                WHEN ABS(t.sku_list_price_mode - e.expected_list_mode) < 0.001 THEN 'PASS'
                WHEN t.sku_list_price_mode IS NULL AND e.expected_list_mode IS NULL THEN 'PASS'
                ELSE 'FAIL'
            END as list_mode_match,
            CASE 
                WHEN ABS(t.sku_online_price_mode - e.expected_online_mode) < 0.001 THEN 'PASS'
                WHEN t.sku_online_price_mode IS NULL AND e.expected_online_mode IS NULL THEN 'PASS'
                ELSE 'FAIL'
            END as online_mode_match
        FROM target_mode t
        LEFT JOIN expected_mode e ON (
            t.sku_id = e.sku_id AND 
            t.region = e.region AND 
            t.month_dt = e.month_dt
        )
        WHERE (ABS(t.sku_list_price_mode - e.expected_list_mode) >= 0.001 
               OR ABS(t.sku_online_price_mode - e.expected_online_mode) >= 0.001)
        LIMIT {SAMPLE_LIMIT}
        """
        
        mismatch_result = impala.get_pandas_df(mode_query)
        
        if len(mismatch_result) == 0:
            print("✅ 众数计算逻辑验证通过：所有众数计算都正确")
        else:
            print("❌ 众数计算逻辑验证失败：存在众数不匹配的记录")
            print("不匹配记录示例：")
            print(mismatch_result.to_string())
            return False
            
    except Exception as e:
        print(f"❌ 众数计算逻辑验证失败: {e}")
        return False
    
    return True

def validate_data_completeness():
    """
    验证数据完整性：确保所有有价格数据的记录都被包含
    """
    print(f"\n=== 📈 数据完整性验证 ===")
    
    try:
        impala = new_impala_connector(emr=True)
        
        # 验证数据完整性
        completeness_query = f"""
        WITH upstream_count AS (
            SELECT COUNT(DISTINCT CONCAT(sku_id, '|', region)) as upstream_count
            FROM {UPSTREAM_TABLE}
            WHERE date_format(dt, 'yyyy-MM-01') = '{TEST_MONTH}'
              AND region IS NOT NULL AND region != ''
              AND sku_id IS NOT NULL AND sku_id != ''
              AND (sku_online_price IS NOT NULL OR sku_list_price IS NOT NULL)
        ),
        target_count AS (
            SELECT COUNT(DISTINCT CONCAT(sku_id, '|', region)) as target_count
            FROM {DATABASE}.{TABLE}
            WHERE month_dt = '{TEST_MONTH}'
        )
        SELECT 
            u.upstream_count,
            t.target_count,
            CASE 
                WHEN u.upstream_count = t.target_count THEN '✅'
                ELSE '❌'
            END as completeness_check
        FROM upstream_count u
        CROSS JOIN target_count t
        """
        
        completeness_result = impala.get_pandas_df(completeness_query)
        
        if len(completeness_result) > 0:
            row = completeness_result.iloc[0]
            upstream_count = row['upstream_count']
            target_count = row['target_count']
            completeness_check = row['completeness_check']
            
            print(f"上游有价格数据记录数: {upstream_count}")
            print(f"目标表记录数: {target_count}")
            
            if completeness_check == '✅':
                print("✅ 数据完整性验证通过：记录数匹配")
            else:
                print("❌ 数据完整性验证失败：记录数不匹配")
                return False
        else:
            print("❌ 数据完整性验证失败：无法获取记录数")
            return False
            
    except Exception as e:
        print(f"❌ 数据完整性验证失败: {e}")
        return False
    
    return True

def validate_field_types():
    """
    验证字段类型正确性
    """
    print(f"\n=== 🔧 字段类型验证 ===")
    
    try:
        impala = new_impala_connector(emr=True)
        
        # 验证字段类型
        field_types_query = f"""
        SELECT 
            sku_id,
            region,
            sku_list_price,
            sku_online_price,
            sku_list_price_min,
            sku_list_price_max,
            sku_list_price_avg,
            sku_online_price_min,
            sku_online_price_max,
            sku_online_price_avg,
            CASE 
                WHEN sku_list_price IS NULL OR sku_list_price >= 0 THEN '✅'
                ELSE '❌'
            END as list_price_type_check,
            CASE 
                WHEN sku_online_price IS NULL OR sku_online_price >= 0 THEN '✅'
                ELSE '❌'
            END as online_price_type_check,
            CASE 
                WHEN sku_list_price_min IS NULL OR sku_list_price_min >= 0 THEN '✅'
                ELSE '❌'
            END as list_min_type_check,
            CASE 
                WHEN sku_online_price_min IS NULL OR sku_online_price_min >= 0 THEN '✅'
                ELSE '❌'
            END as online_min_type_check
        FROM {DATABASE}.{TABLE}
        WHERE month_dt = '{TEST_MONTH}'
          AND (sku_list_price < 0 OR sku_online_price < 0 
               OR sku_list_price_min < 0 OR sku_online_price_min < 0)
        LIMIT {SAMPLE_LIMIT}
        """
        
        type_result = impala.get_pandas_df(field_types_query)
        
        if len(type_result) == 0:
            print("✅ 字段类型验证通过：所有价格字段类型正确")
        else:
            print("❌ 字段类型验证失败：存在类型错误的记录")
            print("类型错误记录示例：")
            print(type_result.to_string())
            return False
            
    except Exception as e:
        print(f"❌ 字段类型验证失败: {e}")
        return False
    
    return True

def validate_currency_consistency():
    """
    验证货币字段一致性
    """
    print(f"\n=== 💱 货币字段一致性验证 ===")
    
    try:
        impala = new_impala_connector(emr=True)
        
        # 验证货币字段一致性
        currency_query = f"""
        SELECT 
            sku_id,
            region,
            sku_list_price_currency,
            sku_online_price_currency,
            CASE 
                WHEN sku_list_price_currency IS NOT NULL AND sku_list_price_currency != '' THEN '✅'
                WHEN sku_list_price IS NULL THEN '✅'
                ELSE '❌'
            END as list_currency_check,
            CASE 
                WHEN sku_online_price_currency IS NOT NULL AND sku_online_price_currency != '' THEN '✅'
                WHEN sku_online_price IS NULL THEN '✅'
                ELSE '❌'
            END as online_currency_check
        FROM {DATABASE}.{TABLE}
        WHERE month_dt = '{TEST_MONTH}'
          AND ((sku_list_price IS NOT NULL AND (sku_list_price_currency IS NULL OR sku_list_price_currency = ''))
               OR (sku_online_price IS NOT NULL AND (sku_online_price_currency IS NULL OR sku_online_price_currency = '')))
        LIMIT {SAMPLE_LIMIT}
        """
        
        currency_result = impala.get_pandas_df(currency_query)
        
        if len(currency_result) == 0:
            print("✅ 货币字段一致性验证通过：所有价格字段都有对应的货币")
        else:
            print("❌ 货币字段一致性验证失败：存在价格字段缺少货币信息")
            print("错误记录示例：")
            print(currency_result.to_string())
            return False
            
    except Exception as e:
        print(f"❌ 货币字段一致性验证失败: {e}")
        return False
    
    return True

def main():
    """
    主验证函数
    """
    print("=" * 60)
    print(f"🔍 {TABLE} 表数据验证开始")
    print(f"📅 测试月份: {TEST_MONTH}")
    print("=" * 60)
    
    init_logging()
    
    validation_results = []
    
    # 执行各项验证
    validation_results.append(("数据质量验证", validate_data_quality()))
    validation_results.append(("主键唯一性验证", validate_primary_key()))
    validation_results.append(("价格聚合逻辑验证", validate_price_aggregation_logic()))
    validation_results.append(("默认价格逻辑验证", validate_default_price_logic()))
    validation_results.append(("中位数字段验证", validate_midrange_fields()))
    validation_results.append(("众数计算逻辑验证", validate_mode_calculation()))
    validation_results.append(("数据完整性验证", validate_data_completeness()))
    validation_results.append(("字段类型验证", validate_field_types()))
    validation_results.append(("货币字段一致性验证", validate_currency_consistency()))
    
    # 输出验证结果汇总
    print("\n" + "=" * 60)
    print("📋 验证结果汇总")
    print("=" * 60)
    
    passed_count = 0
    total_count = len(validation_results)
    
    for validation_name, result in validation_results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"{validation_name}: {status}")
        if result:
            passed_count += 1
    
    print(f"\n📊 验证结果: {passed_count}/{total_count} 项通过")
    
    if passed_count == total_count:
        print("🎉 所有验证项目都通过！数据质量良好。")
        return True
    else:
        print("⚠️  部分验证项目失败，请检查数据质量。")
        return False

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)
