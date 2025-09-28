# dim_sku_ori 表字段映射文档

## 表级配置

| 配置项 | 值 |
|--------|-----|
| **表名** | `datahub_amazon.dim_sku_ori` |
| **清洗频率** | 日度 |
| **更新方式** | 日度全量 |
| **主键** | `sku_id + region` |
| **分区字段** | `region` |
| **上游表** | `datahub_amazon.dwd_sku_daily` |
| **筛选逻辑** | 无 |
| **去重逻辑** | 无 |

## 字段映射逻辑

| 字段名 | 类型 | 上游字段+逻辑 |
|--------|------|------|
| **product_id** | string | product_id |
| **sku_id** | string | sku_id (主键) 取各字段非空非""最新一条记录|
| **product_title** | string | product_title |
| **url** | string | url |
| **color** | string | color |
| **size** | string | size | 
| **brand** | string | brand |
| **brand_id** | bigint | brand_id |
| **std_brand_name** | string | std_brand_name |
| **manufacturer** | string | manufacturer |
| **has_sku** | int | has_sku |
| **variant_information** | string | variant_information |
| **category** | string | category |
| **sub_category** | string | sub_category (用对应category的那一整条数据中的sub_category) |
| **category_id** | string | 留空 |
| **category_name** | string | 最后一个> 后面的 |
| **category_1_id** | int | 留空 |
| **category_1_name** | string | 留空 |
| **category_2_id** | int | 留空 |
| **category_2_name** | string | split_part(sub_category,'>',1) 如果没有则为留空 |
| **category_3_id** | int | 留空 |
| **category_3_name** | string | split_part(sub_category,'>',2) 如果没有则为留空 |
| **category_4_id** | int | 留空 |
| **category_4_name** | string | split_part(sub_category,'>',3) 如果没有则为留空 |
| **category_5_id** | int | 留空 |
| **category_5_name** | string | split_part(sub_category,'>',4) 如果没有则为留空 |
| **category_6_id** | int | 留空 |
| **category_6_name** | string | split_part(sub_category,'>',5) 如果没有则为留空 |
| **category_7_id** | int | 留空 |
| **category_7_name** | string | split_part(sub_category,'>',6) 如果没有则为留空 |
| **category_8_id** | int | 留空 |
| **category_8_name** | string | split_part(sub_category,'>',7) 如果没有则为留空 |
| **category_9_id** | int | 留空 |
| **category_9_name** | string | split_part(sub_category,'>',8) 如果没有则为留空 |
| **seller** | string | seller |
| **seller_id** | string | seller_id |
| **first_image** | string | first_image |
| **imags** | string | imags |
| **video** | string | video |
| **specifications** | string | specifications |
| **additional_description** | string | additional_description |
| **extra_json** | string | extra_json |
| **etl_source** | string | etl_source |
| **first_snapshot_dt** | string | min(dt) group by sku_id |
| **last_snapshot_time** | string | 最晚一条snapshot_time |
| **region** | string | region |

## 核心业务逻辑

### 主键逻辑
- **sku_id**: 主键，取各字段非空非""最新一条记录
- **region**: 分区字段，按地区分区

### 字段选择逻辑
- **去重策略**: 按 `sku_id + region` 分组，每个字段独立取非空非""最新一条记录
- **字段选择**: 对于每个字段（如product_title、brand、color等），按snapshot_time最晚排序
- **时间逻辑**: 
  - `first_snapshot_dt`: 取 `min(dt) group by sku_id`
  - `last_snapshot_time`: 取最晚一条 `snapshot_time`

### 特殊字段处理
- **category**: category
- **sub_category**: 用对应category的那一整条数据中的sub_category
- **category拆分逻辑**: 基于sub_category按'>'分割
  - `category_id`: 留空
  - `category_name`: sub_category最后一个> 后面的 
  - `category_1`: 留空 
  - `category_1_name`: category
  - `category_2`: 留空 
  - `category_2_name`: `split_part(sub_category,'>',1)` 如果没有则为留空
  - `category_3`: 留空
  - `category_3_name`: `split_part(sub_category,'>',2)` 如果没有则为留空
  - `category_4`: 留空
  - `category_4_name`: `split_part(sub_category,'>',3)` 如果没有则为留空
  - `category_5`: 留空
  - `category_5_name`: `split_part(sub_category,'>',4)` 如果没有则为留空
  - `category_6`: 留空
  - `category_6_name`: `split_part(sub_category,'>',5)` 如果没有则为留空
  - `category_7`: 留空
  - `category_7_name`: `split_part(sub_category,'>',6)` 如果没有则为留空
  - `category_8`: 留空
  - `category_8_name`: `split_part(sub_category,'>',7)` 如果没有则为留空
  - `category_9`: 留空
  - `category_9_name`: `split_part(sub_category,'>',8)` 如果没有则为留空

- **dt**: 取最小值 `min(dt) group by sku_id`

## 数据质量要求

### 主键约束
- `sku_id + region` 组合必须唯一
- 避免重复数据

### 数据完整性
- 确保所有有数据的 `sku_id` 都被包含
- 字段选择基于非空非""条件

### 时间字段逻辑
- `first_snapshot_dt`: 记录该SKU第一次出现的时间
- `last_snapshot_time`: 记录该SKU最后一次更新的时间

## 开发/核验注意事项

1. **主键约束**: `sku_id + region` 确保唯一性，避免重复数据
2. **分区优化**: 按 `region` 分区，提高查询效率
3. **字段选择**: 基于非空非""条件选择最新记录
4. **时间逻辑**: 正确处理 `first_snapshot_dt` 和 `last_snapshot_time`
5. **category拆分逻辑**: 
   - 使用 `split_part(sub_category,'>',N)` 函数按'>'分割sub_category
   - 分割位置1-8对应category_2_name到category_9_name
   - 如果分割结果为空，则字段留空
   - 所有category_X_id字段全部留空
6. **数据验证**: 确保sub_category格式正确，包含足够的'>'分隔符
