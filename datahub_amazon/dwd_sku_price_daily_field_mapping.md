# dwd_sku_price_daily 表字段映射文档

## 表级配置

| 配置项 | 值 |
|--------|-----|
| **表名** | `datahub_amazon.dwd_sku_price_daily` |
| **清洗频率** | 日度 |
| **更新方式** | 日度增量 |
| **主键** | `sku_id + region + dt` |
| **分区字段** | `dt + region` |
| **上游表** | `datahub_amazon.dwd_sku_info` |

## 字段映射逻辑

| 字段名 | 类型 | 逻辑 |
|--------|------|------|
| **product_id** | string | 直接映射 |
| **sku_id** | string | 直接映射 |
| **sku_online_price** | double |直接映射 | 取当天非空非null的最小值 |
| **sku_online_price_currency** | string | 直接映射 |取当天非空非null的最晚一条 |
| **sku_list_price** | double | 直接映射 |取当天非空非null的最小值 |
| **sku_list_price_currency** | string |直接映射 | 取当天非空非null的最晚一条 |
| **price_info** | string | `{"sku_online_price":"etl_source","sku_list_price":"etl_source"}` |
| **dt** | string | 分区字段 |
| **region** | string | 分区字段 |

## 核心业务逻辑

### 字段
- **sku_online_price**:  取sku_online_price字段 当天非空非null的最小值
- **sku_online_price_currency**: 取sku_online_price_currency 字段当天非空非null的最晚一条
- **sku_list_price**: 取sku_list_price字段 当天非空非null的最小值
- **sku_list_price_currency**: 取sku_list_price_currency字段 当天非空非null的最晚一条
- **price_info**: JSON格式记录etl_source来源

## SQL实现逻辑

### 字段优化
```sql
-- 在线价格：取最小值
MIN(CASE 
    WHEN sku_online_price IS NOT NULL AND sku_online_price > 0 
    THEN sku_online_price 
    ELSE NULL 
END) as sku_online_price,

-- 标价：取最小值
MIN(CASE 
    WHEN sku_list_price IS NOT NULL AND sku_list_price > 0 
    THEN sku_list_price 
    ELSE NULL 
END) as sku_list_price
```
```sql
-- 在线价格币种：取最晚记录
FIRST_VALUE(sku_online_price_currency) OVER (
    PARTITION BY sku_id, region, dt 
    ORDER BY if(sku_online_price_currency is not null and sku_online_price_currency !='', 1, 0) desc, 
             {priority_case_statement}, snapshot_time DESC
) as sku_online_price_currency,

-- 标价币种：取最晚记录
FIRST_VALUE(sku_list_price_currency) OVER (
    PARTITION BY sku_id, region, dt 
    ORDER BY if(sku_list_price_currency is not null and sku_list_price_currency !='', 1, 0) desc, 
             {priority_case_statement}, snapshot_time DESC
) as sku_list_price_currency
```

```python
def price_info_udf(online_price_source, list_price_source):
    price_info = {}
    if online_price_source:
        price_info["sku_online_price"] = online_price_source
    if list_price_source:
        price_info["sku_list_price"] = list_price_source
    return json.dumps(price_info, ensure_ascii=False)
```
## 实现要点
1. **主键约束**: `sku_id + region + dt` 确保唯一性
2. **分区优化**: 按日期和地区分区，提高查询效率
