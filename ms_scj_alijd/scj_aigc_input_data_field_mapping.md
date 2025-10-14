# SCJ AIGC输入数据表字段映射文档

## 表结构概述
- **表名**: `ms_scj_alijd.scj_aigc_input_data`
- **主键**: `platform + item_id + pfsku_id + month_dt`
- **分区字段**: `month_dt` (STRING)
- **存储格式**: PARQUET
- **压缩格式**: SNAPPY

## 数据源说明

### 基础数据查询
```sql
SELECT t0.*, t1.tags
FROM ms_scj_alijd.monthly_sales_wide t0 
LEFT JOIN ms_scj_alijd.std_mapping t1 ON t0.unique_id = t1.unique_id
WHERE t0.pfsku_value_sales > 0 
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
```

## 字段详细说明

| 序号 | 字段名 | 数据类型 | 说明 | 来源字段 | 清洗规则 |
|------|--------|----------|------|----------|----------|
| 1 | platform | STRING | 平台 | platform | 平台标识 |
| 2 | item_id | BIGINT | 商品ID | item_id | 商品唯一标识 |
| 3 | pfsku_id | BIGINT | SKU ID | pfsku_id | SKU唯一标识 |
| 4 | item_title | STRING | 商品标题 | item_title | 商品标题 |
| 5 | pfsku_title | STRING | 子产品标题 | pfsku_title | SKU标题 |
| 6 | first_image | STRING | 首图 | item_image | 商品首图URL |
| 7 | pfsku_image | STRING | 子产品图片 | pfsku_image | SKU图片URL |
| 8 | category_name | STRING | 原始品类名称 | category_name | 原始品类名称 |
| 9 | shop_name | STRING | 店铺名称 | shop_name | 店铺名称 |
| 10 | brand_name | STRING | 品牌 | brand_name | 品牌名称 |
| 11 | item_url | STRING | 产品链接 | item_url | 商品链接URL |
| 12 | pfsku_url | STRING | 子产品链接 | pfsku_url | SKU链接URL |
| 13 | tags | STRING | 标签 | tags | 商品标签 |
| 14 | pfsku_value_sales | DOUBLE | 销售额 | pfsku_value_sales | 销售额数据 |
| 15 | pfsku_unit_sales | INT | 销量 | pfsku_unit_sales | 销量数据 |
| 16 | pfsku_discount_price | DOUBLE | 价格 | pfsku_discount_price | 折扣价格 |
| 17 | top80 | DOUBLE | 累计销售额占比 | - | 按partition by platform、month_dt order by pfsku_value_sales desc 排序计算累计占比 |
| 18 | month_start | STRING | 起始月份 | - | 基于hash_id比较计算的起始月份 |
| 19 | month_dt | STRING | 月份 | month_dt | 数据月份分区 |

## 复杂字段计算逻辑

### 1. top80 字段计算
**计算规则**: 按 `platform`、`month_dt` 分组，按 `pfsku_value_sales` 降序排列，计算累计销售额占比

```sql
-- 计算逻辑示例
SELECT 
  platform,
  month_dt,
  item_id,
  pfsku_id,
  pfsku_value_sales,
  SUM(pfsku_value_sales) OVER (
    PARTITION BY platform, month_dt 
    ORDER BY pfsku_value_sales DESC 
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) / SUM(pfsku_value_sales) OVER (PARTITION BY platform, month_dt) AS top80
FROM source_data
```

### 2. month_start 字段计算

#### Hash ID 生成函数
```python
import hashlib
from typing import Optional

def generate_hash_id(item_title: Optional[str], pfsku_title: Optional[str]) -> str:
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
```

#### month_start 计算规则

**Setup 阶段** (month_dt <= '2025-08-01'):
- `month_start = '2022-07-01'`

**Ongoing 阶段** (从2509开始，即month_dt > ='2025-09-01'):
- 按 `platform`、`item_id`、`pfsku_id` 分组
- 比较当前月份 `month_dt` 的 `hash_id` 与邻近月份的值：
  - 如果 `hash_id` 相等：`month_start = 邻近月份的month_start`（被第一步处理之后的值）
  - 如果 `hash_id` 不相等或当前月为首月：`month_start = 当前月份month_dt`

```sql
-- month_start 计算逻辑示例 - 分两步处理
WITH hash_comparison AS (
  SELECT 
    platform, item_id, pfsku_id, month_dt,
    generate_hash_id(item_title, pfsku_title) AS hash_id
  FROM source_data
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
  *,
  month_start
FROM month_start_final
```

## 数据处理流程

### 1. 数据过滤
- 过滤条件：`pfsku_value_sales > 0`
- 排除特定品类：不在 `category_name` 和 `category_1` 排除列表中

### 2. 字段映射
- 基础字段直接映射
- 特殊字段重命名：
  - `item_image` → `first_image`

### 3. 复杂字段计算
- 计算 `top80`：累计销售额占比
- 计算 `month_start`：基于hash_id比较的起始月份

### 4. 数据输出
- 按 `month_dt` 分区存储
- 使用 PARQUET + SNAPPY 压缩格式

## 更新历史
- **2025-01-XX**: 初始版本，定义基础表结构和字段映射
- **2025-01-XX**: 完善复杂字段计算逻辑，包括top80和month_start字段
- **2025-01-XX**: 优化hash_id生成函数和month_start计算规则
