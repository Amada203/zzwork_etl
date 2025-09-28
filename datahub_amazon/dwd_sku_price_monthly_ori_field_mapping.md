# dwd_sku_price_monthly_ori 表字段映射文档

## 表级配置

| 配置项 | 值 |
|--------|-----|
| **表名** | `datahub_amazon.dwd_sku_price_monthly_ori` |
| **清洗频率** | 月度 |
| **更新方式** | 月度增量 |
| **主键** | `sku_id + region + month_dt` |
| **分区字段** | `region + month_dt` |
| **上游表** | `datahub_amazon.dwd_sku_price_daily` |
| **筛选逻辑** | 时间区间取上游表优先选飞书表里面的日期，其次用默认t-1月第一天到t-1月最后一天 |

## 字段映射逻辑

| 字段名 | 类型 | 上游字段+逻辑 |
|--------|------|------|
| **product_id** | string | 优先取对应最小价格记录的非空product_id，如果为空则取其他非空非''的product_id |
| **sku_id** | string | sku_id (主键) |
| **sku_list_price** | double | sku_list_price_min (默认值) |
| **sku_list_price_currency** | string | 取对应最小价格记录的币种，优先取非空币种 |
| **sku_online_price** | double | sku_online_price_min (默认值) |
| **sku_online_price_currency** | string | 取对应最小价格记录的币种，优先取非空币种 |
| **sku_list_price_min** | double | min(有效价格) - 只计算非空且>0的价格 |
| **sku_list_price_max** | double | max(有效价格) - 只计算非空且>0的价格 |
| **sku_list_price_avg** | double | avg(有效价格) - 只计算非空且>0的价格 |
| **sku_list_price_mode** | double | 众数,取出现次数最多价格,如果有多个价格出现次数一致,取最低价 |
| **sku_list_price_midrange** | double | 留空 |
| **sku_online_price_min** | double | min(有效价格) - 只计算非空且>0的价格 |
| **sku_online_price_max** | double | max(有效价格) - 只计算非空且>0的价格 |
| **sku_online_price_avg** | double | avg(有效价格) - 只计算非空且>0的价格 |
| **sku_online_price_mode** | double | 众数,取出现次数最多价格,如果有多个价格出现次数一致,取最低价 |
| **sku_online_price_midrange** | double | 留空 |
| **region** | string | region (分区字段) |
| **month_dt** | string | month_dt (分区字段) |

## 核心业务逻辑

### 聚合逻辑
- **分组维度**: 按 `sku_id + region + month_dt` 分组
- **时间范围**: 月度数据聚合
- **数据源**: 从 `dwd_sku_price_daily` 日度表聚合

### 价格字段处理
- **默认价格字段**:
  - `sku_list_price`: 取 `sku_list_price_min` 作为默认值
  - `sku_online_price`: 取 `sku_online_price_min` 作为默认值

- **统计价格字段**:
  - `sku_list_price_min`: `min(有效价格)` 月度最低价（只计算非空且>0的价格）
  - `sku_list_price_max`: `max(有效价格)` 月度最高价（只计算非空且>0的价格）
  - `sku_list_price_avg`: `avg(有效价格)` 月度平均价（只计算非空且>0的价格）
  - `sku_list_price_mode`: 众数价格，取出现次数最多价格，如果有多个价格出现次数一致，取最低价
  - `sku_list_price_midrange`: 留空（按文档要求，不计算中位数）

  - `sku_online_price_min`: `min(有效价格)` 月度最低价（只计算非空且>0的价格）
  - `sku_online_price_max`: `max(有效价格)` 月度最高价（只计算非空且>0的价格）
  - `sku_online_price_avg`: `avg(有效价格)` 月度平均价（只计算非空且>0的价格）
  - `sku_online_price_mode`: 众数价格，取出现次数最多价格，如果有多个价格出现次数一致，取最低价
  - `sku_online_price_midrange`: 留空（按文档要求，不计算中位数）

### 币种和Product ID选择逻辑
- **币种选择优先级**:
  1. 优先选择与最小价格匹配记录的币种
  2. 如果最小价格记录币种为空，选择其他非空币种
  3. 按时间倒序排序（最新记录优先）
- **Product ID选择优先级**:
  1. 优先选择与最小价格匹配记录的非空product_id
  2. 如果最小价格记录product_id为空，选择其他非空非''的product_id
  3. 按时间倒序排序（最新记录优先）

### 众数计算逻辑
- **众数定义**: 取出现次数最多的价格
- **平局处理**: 如果有多个价格出现次数一致，取最低价
- **实现方式**: 使用窗口函数 `ROW_NUMBER() OVER (PARTITION BY sku_id, region, month_dt ORDER BY COUNT(price) DESC, price ASC)`

### 中位数字段处理
- **文档要求**: `sku_list_price_midrange` 和 `sku_online_price_midrange` 字段留空
- **实现说明**: 脚本中保留计算逻辑 `(max + min) / 2` 但设为注释状态，按文档要求输出 NULL 值

### 时间筛选逻辑
- **优先级1**: 使用飞书表中的日期配置
  - 表名: `datahub_amazon`
  - Sheet名: `monthly_price_time_range`
  - 字段: `month_dt`, `start_dt`, `end_dt`
  - 逻辑: 根据t-1月匹配飞书表格中的 `month_dt`，使用对应的 `start_dt` 和 `end_dt`
- **优先级2**: 使用默认时间范围
  - 开始时间: t-1月第一天
  - 结束时间: t-1月最后一天
  - 逻辑: 当飞书表格配置无效时，自动计算t-1月的时间范围
  - 说明: t-1月 = 昨天所在月份的上一个月

## 数据清洗逻辑

### 价格数据清洗
- **有效价格定义**: 只计算 `IS NOT NULL AND > 0` 的价格
- **统计计算**: 所有 min/max/avg 统计都基于有效价格
- **默认价格**: 取有效价格的最小值作为默认价格

### 币种和ID清洗
- **币种清洗**: 过滤空字符串和NULL值
- **Product ID清洗**: 过滤空字符串和NULL值
- **优先级选择**: 基于最小价格记录优先，其次按时间倒序

## 开发/核验注意事项

1. **主键约束**: `sku_id + region + month_dt` 确保唯一性，避免重复数据
2. **分区优化**: 按 `region + month_dt` 分区，提高查询效率
3. **聚合逻辑**: 确保月度聚合逻辑正确
4. **众数计算**: 实现众数计算逻辑，处理平局情况
5. **时间筛选**: 优先使用飞书表配置，其次使用默认时间范围
6. **数据验证**: 确保价格字段不为负数，货币字段有效
7. **性能优化**: 考虑使用窗口函数优化众数计算
8. **空值处理**: 合理处理空值和无效价格数据
9. **数据清洗**: 确保只计算有效价格（>0），过滤无效数据
10. **字段选择**: 确保币种和product_id选择逻辑符合业务需求