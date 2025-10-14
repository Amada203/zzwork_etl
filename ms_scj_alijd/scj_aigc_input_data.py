# -*- coding: utf-8 -*-
"""
SCJ AIGC输入数据脚本：基于monthly_sales_wide生成scj_aigc_input_data表
主要逻辑思路：
1. 从monthly_sales_wide和std_mapping表获取基础数据，应用品类过滤条件
2. 基于item_title和pfsku_title生成hash_id用于month_start计算
3. 计算top80字段：按platform、month_dt分组，按pfsku_value_sales降序计算累计占比
4. 计算month_start字段：setup阶段统一为2022-07-01，ongoing阶段基于hash_id比较
5. 输出到scj_aigc_input_data表，按month_dt分区存储

增量全量模式说明：
- 增量模式(update_mode='incremental')：只处理指定月份数据，用于日常增量更新
- 全量模式(update_mode='full')：处理所有历史数据，用于初始化或重新计算
- 参数配置：{{ update_mode }}, {{ month_dt }}, {{ project_start_month }}

压缩格式说明：
- 使用GZIP压缩，平衡查询性能和存储成本
- 查询速度快，适合频繁查询场景，后期定期查询也能快速响应
"""

import hashlib
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when, isnan, isnull, sum as spark_sum, row_number, lag
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.window import Window
from pigeon.connector import new_impala_connector


spark = SparkSession.builder.appName("onework.{{dag_name}}.{{job_name}}.{{ execution_date }}").enableHiveSupport().getOrCreate()
impala = new_impala_connector()

# 源表和目标表
source_table_1 = 'ms_scj_alijd.monthly_sales_wide'
source_table_2 = 'ms_scj_alijd.std_mapping'
result_table = 'ms_scj_alijd.scj_aigc_input_data'

# 增量全量模式配置
update_mode = '{{ update_mode }}'
month_dt = '{{ month_dt }}'
project_start_month = '{{ project_start_month }}'

# 增量更新 或者 全量更新
if update_mode == 'incremental':
    month_condition = f't0.month_dt = "{month_dt}"'
    month_partition = f'month_dt = "{month_dt}"'  # 分区子句
    drop_incremental_stats = f'DROP INCREMENTAL STATS {result_table} PARTITION(month_dt="{month_dt}");'
elif update_mode == 'full':
    month_condition = '1=1'
    month_partition = 'month_dt'  # 动态分区
    drop_incremental_stats = f'DROP INCREMENTAL STATS {result_table} PARTITION(month_dt>="{project_start_month}");'
else:
    raise ValueError(f'update_mode="{update_mode}" is invalid !!!')


# Hash函数定义
def _generate_hash_id(item_title: str, pfsku_title: str) -> str:
    """
    生成hash ID
    需要strip一下，原始标题有制表符，还有换行及多个空格
    
    Args:
        item_title: 商品标题，可以为None、NaN或其他类型
        pfsku_title: SKU标题，可以为None、NaN或其他类型
        
    Returns:
        hash ID字符串 (32位MD5)
    """
    import pandas as pd
    
    # 处理各种类型的输入（None、NaN、float等）
    def safe_str_strip(value):
        if value is None or pd.isna(value):
            return ""
        return str(value).strip()
    
    # 将换行及多个空格替换为单个空格
    item_title = ' '.join(safe_str_strip(item_title).split())
    pfsku_title = ' '.join(safe_str_strip(pfsku_title).split())
    
    # 组合字符串并生成MD5 hash
    combined = f"{item_title}_{pfsku_title}"
    return hashlib.md5(combined.encode('utf-8')).hexdigest()


# 目标表DDL
DDL = f'''
CREATE TABLE IF NOT EXISTS {result_table} (
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
    pfsku_value_sales DOUBLE,
    pfsku_unit_sales INT,
    pfsku_discount_price DOUBLE,
    top80 DOUBLE,
    month_start STRING,
    hash_id STRING
) PARTITIONED BY (month_dt STRING) STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='GZIP')
'''


# 注册UDF函数
generate_hash_id_udf = udf(_generate_hash_id, StringType())

# 基础数据查询 - 包含所有过滤条件
base_query = f'''
SELECT 
    t0.platform,
    t0.item_id,
    t0.pfsku_id,
    t0.item_title,
    t0.pfsku_title,
    t0.item_image as first_image,
    t0.pfsku_image,
    t0.category_name,
    t0.shop_name,
    t0.brand_name,
    t0.item_url,
    t0.pfsku_url,
    t0.pfsku_value_sales,
    t0.pfsku_unit_sales,
    t0.pfsku_discount_price,
    t1.tags,
    t0.month_dt
FROM {source_table_1} t0
LEFT JOIN {source_table_2} t1 ON t0.unique_id = t1.unique_id
WHERE t0.pfsku_value_sales > 0
  AND {month_condition} 
  AND t0.category_name NOT IN (
    "眼脸部防护", "身体防护", "公牛", "得力（deli）", "罗赛吉尔", "中分",
    "照明", "监控设备", "工业通讯", "工控传感器", "吸附用品", "地垫及矿棉板",
    "垃圾处理设施", "工业擦拭", "通信/光缆", "紧固件", "保护器", "插座",
    "中元华电", "福禄克（FLUKE）", "金思丹博", "安防监控", "应急处理",
    "蓝宇星琳", "鑫广和", "分析检测", "PLC", "波斯（BoSi）", "其它日用",
    "宠物吸毛器", "宠物尿垫/纸尿裤", "宠物粘毛", "尿垫", "特殊商品",
    "狗厕所", "猫砂", "猫砂盆", "收纳架/篮", "厨具清洁剂", "除醛果冻",
    "鞋油", "普通洗衣液", "开关", "其它仪表", "环境检测", "车间化学品",
    "安全器具", "门禁/闸机/停车场设施", "应急照明", "大垃圾桶",
    "工业扫地机/车", "工业洗地机/车", "干地机/吹干机", "扫把",
    "扫雪机/车", "清洁推车/布草车", "锄铲工具", "模拟演习", "鱼线",
    "军迷用品", "户外仪表", "户外工具", "医用垃圾袋", "商用保洁工具套组",
    "商用刮水器", "商用垃圾桶", "商用垃圾袋", "商用清洁推车",
    "商用电器清洁剂", "大盘卷纸", "定制纸巾", "商用干洗剂", "商用柔顺剂",
    "商用洗衣液", "商用洗衣粉", "商用漂白剂", "商用空气治理/芳香用品",
    "化妆工具清洁剂", "灯具清洁剂", "纱窗清洁剂", "运动器材清洁剂",
    "餐具光亮剂", "皮革上光剂", "其他杀虫灭害产品", "灭蟑螂剂", "电蚊香器",
    "其他清洁工具", "家务手套", "抹布", "拖把/配件", "清洁刷", "百洁布",
    "钢丝球", "除尘工具", "固体空气清香剂", "空气清香剂喷雾", "电蚊香液",
    "有价优惠券", "儿童餐具", "洗漱杯", "桌面清洁套装", "水桶",
    "居家日用套装", "香包/香囊", "车用空气净化/清新剂", "空气芳香剂",
    "香薰喷雾剂", "香薰香料", "其它日用家电"
  )
  AND t0.category_1 NOT IN (
    "五金/工具", "京五盟-水暖配件", "元器件", "医药", "厨具", "图书",
    "家具", "家用电器", "家纺", "家装建材", "居家布艺", "床上用品",
    "数字内容", "数码", "文娱", "水饮冲调", "汽车用品", "灯饰照明",
    "生鲜", "电脑、办公", "美妆护肤", "鞋靴", "食品饮料",
    "ZIPPO/瑞士军刀/眼镜", "书籍/杂志/报纸", "医疗器械", "厨房/烹饪用具",
    "咖啡/麦片/冲饮", "女士内衣/男士内衣/家居服", "女鞋", "家装灯饰光源",
    "彩妆/香水/美妆工具", "影音电器", "户外/登山/野营/旅行用品", "收纳整理",
    "模玩/动漫/周边/娃圈三坑/桌游", "水产肉类/新鲜蔬果/熟食", "电子/电工",
    "电脑硬件/显示器/电脑周边", "童装/婴儿装/亲子装", "童鞋/婴儿鞋/亲子鞋",
    "美发护发/假发", "美容护肤/美体/精油", "计生用品", "购物金", "酒类",
    "餐饮具"
  )
'''

# 执行基础查询并添加hash_id字段
base_df = spark.sql(base_query)
base_df_with_hash = base_df.withColumn("hash_id", generate_hash_id_udf(base_df.item_title, base_df.pfsku_title))

# 小文件repartition处理：repartition到200个分区进行数据处理，平衡性能和资源使用
base_df_with_hash = base_df_with_hash.repartition(200)
base_df_with_hash.createOrReplaceTempView('base_data_with_hash')

# 计算top80字段 - 使用窗口函数高效计算累计销售额占比
top80_query = '''
SELECT 
    *,
    SUM(pfsku_value_sales) OVER (
        PARTITION BY platform, month_dt 
        ORDER BY pfsku_value_sales DESC 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) / SUM(pfsku_value_sales) OVER (PARTITION BY platform, month_dt) AS top80
FROM base_data_with_hash
'''

# 计算month_start字段 - 分两步处理：先处理第一步，再基于hash_id修正
month_start_query = '''
WITH hash_comparison AS (
  SELECT 
    platform, item_id, pfsku_id, month_dt, hash_id, top80,
    item_title, pfsku_title, first_image, 
    pfsku_image, category_name, shop_name, brand_name, item_url, 
    pfsku_url, tags, pfsku_value_sales, pfsku_unit_sales, pfsku_discount_price
  FROM (
    SELECT 
        *,
        SUM(pfsku_value_sales) OVER (
            PARTITION BY platform, month_dt 
            ORDER BY pfsku_value_sales DESC 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) / SUM(pfsku_value_sales) OVER (PARTITION BY platform, month_dt) AS top80
    FROM base_data_with_hash
  ) t
),
month_start_calculation AS (
  SELECT 
    *,
    LAG(hash_id) OVER (
      PARTITION BY platform, item_id, pfsku_id 
      ORDER BY month_dt
    ) AS prev_hash_id,
    LAG(month_dt) OVER (
      PARTITION BY platform, item_id, pfsku_id 
      ORDER BY month_dt
    ) AS prev_month_dt,
    -- 第一步：先处理month_start的初始值
    CASE 
      WHEN month_dt <= '2025-08-01' THEN '2022-07-01'
      ELSE month_dt
    END AS initial_month_start
  FROM hash_comparison
),
month_start_final AS (
  SELECT 
    *,
    -- 第二步：基于hash_id比较修正month_start
    CASE 
      WHEN month_dt <= '2025-08-01' THEN initial_month_start
      WHEN month_dt >= '2025-09-01' AND hash_id = prev_hash_id AND prev_month_dt IS NOT NULL THEN 
        -- 如果hash_id相等，继承邻近月份的month_start（被第一步处理之后的值）
        LAG(initial_month_start) OVER (
          PARTITION BY platform, item_id, pfsku_id 
          ORDER BY month_dt
        )
      ELSE initial_month_start
    END AS month_start
  FROM month_start_calculation
)
SELECT 
    platform, item_id, pfsku_id, item_title, pfsku_title, first_image,
    pfsku_image, category_name, shop_name, brand_name, item_url,
    pfsku_url, tags, pfsku_value_sales, pfsku_unit_sales, pfsku_discount_price,
    top80,
    month_start,
    hash_id,
    month_dt
FROM month_start_final
'''

final_df = spark.sql(month_start_query)
# 最终数据repartition到合理分区数，减少小文件数量
# 根据数据量动态调整：增量模式用较少分区，全量模式用较多分区
repartition_num = 2 if update_mode == 'incremental' else 10
final_df = final_df.repartition(repartition_num)
final_df.createOrReplaceTempView('final_data')

# 创建目标表
spark.sql(DDL)
spark.sql('SET spark.sql.parquet.compression.codec=gzip')
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

# 插入数据到目标表
spark.sql(f'''
INSERT OVERWRITE TABLE {result_table}
PARTITION ({month_partition})
SELECT 
    platform,
    item_id,
    pfsku_id,
    item_title,
    pfsku_title,
    first_image,
    pfsku_image,
    category_name,
    shop_name,
    brand_name,
    item_url,
    pfsku_url,
    tags,
    pfsku_value_sales,
    pfsku_unit_sales,
    pfsku_discount_price,
    top80,
    month_start,
    hash_id,
    month_dt
FROM final_data
''')

spark.stop()

# Impala统计更新
impala.execute(f'invalidate metadata {result_table}')
impala.execute(drop_incremental_stats)
impala.execute(f'compute incremental stats {result_table}')
