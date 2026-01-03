# -*- coding: utf-8 -*-
import logging

from dw_util.util import str_utils
from pigeon.connector import new_impala_connector
from pigeon.utils import init_logging
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from spark_util.insert import spark_write_hive

TARGET_DB = '{{ monthly_sales_wide }}'.split('.')[0]
TARGET_TABLE = '{{ monthly_sales_wide }}'.split('.')[1]
CALC_PARTITION = '{{ month_dt }}'
REFRESH_STATS = False
EMR = True
PARTITION_NUM = 600
COMPRESSION = 'gzip'

update_mode = '{{ update_mode }}'
project_start_month = '{{ project_start_month }}'
if update_mode == 'incremental':
    month_field = ',a.month_dt'
    month_condition = f'a.month_dt = "{CALC_PARTITION}"'
    month_partition = f'month_dt = "{CALC_PARTITION}"'
    drop_incremental_stats = f'DROP INCREMENTAL STATS {TARGET_DB}.{TARGET_TABLE} PARTITION(month_dt="{CALC_PARTITION}");'
elif update_mode == 'full':
    month_field = ',a.month_dt'
    month_condition = 'a.month_dt IS NOT NULL'
    month_partition = 'month_dt'
    drop_incremental_stats = f'DROP INCREMENTAL STATS {TARGET_DB}.{TARGET_TABLE} PARTITION(month_dt>="{project_start_month}");'
else:
    raise ValueError(f'update_mode="{update_mode}" is invalid !!!')

DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_DB}.{TARGET_TABLE} (
  unique_id                        bigint,
  platform                         string,
  market                           string,
  category_id                      bigint,
  category_name                    string,
  category_1                       string,
  category_2                       string,
  category_3                       string,
  category_4                       string,
  category_5                       string,
  category_6                       string,
  category_1_id                    string,
  category_2_id                    string,
  category_3_id                    string,
  category_4_id                    string,
  category_5_id                    string,
  category_6_id                    string,
  category_1_cn                    string,
  category_2_cn                    string,
  category_3_cn                    string,
  category_4_cn                    string,
  category_5_cn                    string,
  category_6_cn                    string,
  shop_id                          string,
  shop_name                        string,
  unique_shop_name                 string,
  updated_shop_name                string,
  shop_type                        string,
  shop_url                         string,
  brand_id                         string,
  brand_name                       string,
  ai_brand_name                    string,
  item_id                          string,
  item_url                         string,
  item_title                       string,
  item_image                       string,
  is_multi_pfsku                   string,
  pfsku_id                         string,
  pfsku_url                        string,
  pfsku_title                      string,
  pfsku_title_cn                   string,
  pfsku_image                      string,
  sub_category                     string,
  sub_category_cn                  string,
  is_bundle                        int,
  is_gift                          int,
  sku_src                          string,
  sku_title                        string,
  sku_no                           int,
  sku_num                          int,
  media                            string,
  std_category_name                string,
  std_sub_category_name            string,
  std_brand_name_ori               string,
  std_brand_name                   string,
  manufacturer                     string,
  variant                          string,
  std_spu_name                     string,
  std_sku_name                     string,
  attributes                       string,
  package                          int,
  weight                           double,
  total_weight                     double,
  item_unit_sales                  bigint,
  pfsku_unit_sales                 bigint,
  pfsku_discount_price             double,
  pfsku_page_price                 double,
  pfsku_value_sales                double,
  pfsku_discountprice_value_sales  double,
  pfsku_pageprice_value_sales      double,
  sku_unit_sales                   bigint,
  sku_discount_price_tmp           double,
  sku_discount_price               double,
  sku_value_sales                  double,
  sku_value_ratio                  double,
  sku_value_ratio_src              string,
  sku_value_ratio_fixed            double,
  sku_page_price                   double,
  sku_pageprice_value_sales        double,
  sku_discountprice_value_sales    double,
  sku_volume_sales                 double,
  sku_pack_sales                   double,
  fix_count_ratio                  double,--1030add
  etl_updated_at                   string,
  product_listing_time             string,
  std_category_1                    string,
  std_category_2                    string,
  std_category_3                    string,
  std_category_4                    string,
  std_category_5                    string,
  std_category_6                    string
) PARTITIONED BY (month_dt STRING) COMMENT '结果宽表' STORED AS PARQUET
"""

# 从DDL解析出非分区列顺序，用于必要时的降级写入
QUERY_COLUMNS = str_utils.get_query_column_from_ddl(DDL)


def spark_sql_with_log(spark, query):
    logging.info(query)
    return spark.sql(query)


def dumper(spark):
    query = f"""
WITH t_staging AS (
    SELECT a.unique_id,
           a.platform,
           a.market,
           a.category_id,
           a.category_name,
           a.category_1,
           a.category_2,
           a.category_3,
           a.category_4,
           a.category_5,
           a.category_6,
           a.category_1_id,
           a.category_2_id,
           a.category_3_id,
           a.category_4_id,
           a.category_5_id,
           a.category_6_id,
           a.category_1_cn,
           a.category_2_cn,
           a.category_3_cn,
           a.category_4_cn,
           a.category_5_cn,
           a.category_6_cn,
           a.shop_id,
           a.shop_name,
           a.unique_shop_name,
           a.shop_type,
           a.shop_url,
           a.brand_id,
           a.brand_name,
           a.ai_brand_name,
           a.item_id,
           COALESCE(NULLIF(REGEXP_REPLACE(a.item_url, '/dp/MC_Assembly_1#', '/dp/'), ''), a.item_url) AS item_url,
           a.item_title,
           a.item_image,
           b.is_multi_pfsku,
           a.pfsku_id,
           COALESCE(NULLIF(REGEXP_REPLACE(a.pfsku_url, '/dp/MC_Assembly_1#', '/dp/'), ''), a.pfsku_url) AS pfsku_url,
           a.pfsku_title,
           a.pfsku_title_cn,
           a.pfsku_image,
           a.sub_category,
           a.sub_category_cn,
           a.is_bundle,
           a.is_gift,
           a.sku_src,
           a.sku_title,
           a.sku_no,
           a.sku_num,
           a.media,
           a.std_category_name,
           a.std_sub_category_name,
           a.std_brand_name,
           a.manufacturer,
           a.variant,
           a.std_spu_name,
           a.std_sku_name,
           a.attributes,
           a.PACKAGE,
           a.weight,
           a.total_weight,
           
           b.item_unit_sales,
           b.pfsku_unit_sales,
           c.discount_price AS pfsku_discount_price,
           c.page_price AS pfsku_page_price,
           b.pfsku_unit_sales * c.discount_price AS pfsku_value_sales,
           b.pfsku_unit_sales * c.discount_price AS pfsku_discountprice_value_sales,
           b.pfsku_unit_sales * c.page_price AS pfsku_pageprice_value_sales,
           
           CAST(b.pfsku_unit_sales AS BIGINT) AS sku_unit_sales,
           IF(a.sku_num = 1, c.discount_price / a.`package`, 0) AS sku_discount_price_tmp,
           CAST(NULL AS DOUBLE) AS sku_discount_price,
           CAST(NULL AS DOUBLE) AS sku_value_sales,
           a.sku_value_ratio,
           a.sku_value_ratio_src,
           a.sku_value_ratio AS sku_value_ratio_fixed,
           CAST(NULL AS DOUBLE) AS sku_page_price,
           CAST(NULL AS DOUBLE) AS sku_pageprice_value_sales,
           CAST(NULL AS DOUBLE) AS sku_discountprice_value_sales,
           CAST(b.pfsku_unit_sales AS BIGINT) * a.total_weight AS sku_volume_sales,
           CAST(b.pfsku_unit_sales AS BIGINT) * a.package AS sku_pack_sales,
           b.fix_count_ratio,
          -- CAST(NULL AS STRING) AS etl_updated_at,
           date_format(now(), 'yyyy-MM-dd HH:mm:ss') AS etl_updated_at,
           a.std_category_1,
           a.std_category_2,
           a.std_category_3,
           a.std_category_4,
           a.std_category_5,
           a.std_category_6,
           a.month_dt
    FROM {{ std_mapping }} a
    LEFT JOIN (SELECT * FROM {{ monthly_count_fixed }} a WHERE {month_condition}) b ON a.month_dt = b.month_dt
    AND a.platform = b.platform
    AND a.item_id = b.item_id
    AND a.pfsku_id = b.pfsku_id
    AND (lower(a.platform) != 'amazon' OR a.market = b.market)
    AND IF(lower(a.platform) = 'suning', a.shop_id, '0') = IF(lower(b.platform) = 'suning', b.shop_id, '0')
    LEFT JOIN (SELECT * FROM {{ monthly_price_fixed }} a WHERE {month_condition}) c ON a.month_dt = c.month_dt
    AND a.platform = c.platform
    AND a.item_id = c.item_id
    AND a.pfsku_id = c.pfsku_id
    AND (lower(a.platform) != 'amazon' OR a.market = c.market)
    AND IF(lower(a.platform) = 'suning', a.shop_id, '0') = IF(lower(c.platform) = 'suning', c.shop_id, '0') 
    LEFT ANTI JOIN (SELECT * FROM {{ monthly_product_status }} a WHERE {month_condition}) d ON a.month_dt = d.month_dt
    AND a.platform = d.platform
    AND a.item_id = d.item_id
    AND a.pfsku_id = d.pfsku_id
    AND (lower(a.platform) != 'amazon' OR a.market = d.market)
    AND IF(lower(a.platform) = 'suning', a.shop_id, '0') = IF(lower(d.platform) = 'suning', d.shop_id, '0')
    AND d.status = 'exclude'
    WHERE 1=1
    AND c.page_price > {{ min_price }} AND c.page_price < {{ max_price }}
    AND b.item_unit_sales > {{ min_count }} AND b.item_unit_sales < {{ max_count }}
    AND {month_condition}
),

t_sku_price_1 AS (
  SELECT market as f_market,
         platform as f_platform,
         month_dt as f_month_dt,
         std_sku_name AS f_std_sku_name,
         AVG(sku_discount_price_tmp) AS sku_discount_price_tmp
  FROM t_staging
  WHERE 1=1 
    AND sku_num = 1
    AND std_sku_name IS NOT NULL
    AND sku_discount_price_tmp != 0
  GROUP BY 1, 2, 3, 4
),

t_product_listing_time AS (
  SELECT DISTINCT
         platform AS listing_platform,
         market AS listing_market,
         IF(LOWER(platform) = 'amazon', pfsku_id, item_id) AS listing_key,
         MIN(month_dt) OVER (PARTITION BY platform, market, IF(LOWER(platform) = 'amazon', pfsku_id, item_id)) AS product_listing_time
  FROM {{ std_mapping }}
  WHERE month_dt IS NOT NULL
)
SELECT a.unique_id,
       a.platform,
       a.market,
       a.category_id,
       a.category_name,
       a.category_1,
       a.category_2,
       a.category_3,
       a.category_4,
       a.category_5,
       a.category_6,
       a.category_1_id,
       a.category_2_id,
       a.category_3_id,
       a.category_4_id,
       a.category_5_id,
       a.category_6_id,
       a.category_1_cn,
       a.category_2_cn,
       a.category_3_cn,
       a.category_4_cn,
       a.category_5_cn,
       a.category_6_cn,
       a.shop_id,
       a.shop_name,
       a.unique_shop_name,
       d.updated_shop_name,
       a.shop_type,
       a.shop_url,
       a.brand_id,
       a.brand_name,
       a.ai_brand_name,
       a.item_id,
       a.item_url,
       a.item_title,
       a.item_image,
       a.is_multi_pfsku,
       a.pfsku_id,
       a.pfsku_url,
       a.pfsku_title,
       a.pfsku_title_cn,
       a.pfsku_image,
       a.sub_category,
       a.sub_category_cn,
       a.is_bundle,
       a.is_gift,
       a.sku_src,
       a.sku_title,
       a.sku_no,
       a.sku_num,
       a.media,
       a.std_category_name,
       a.std_sub_category_name,
       a.std_brand_name AS std_brand_name_ori,
       --e.std_brand_name AS std_brand_name,
       COALESCE(e.std_brand_name, a.std_brand_name) AS std_brand_name,
       a.manufacturer,
       a.variant,
       a.std_spu_name,
       a.std_sku_name,
       a.attributes,
       a.PACKAGE,
       a.weight,
       a.total_weight,
       a.item_unit_sales,
       a.pfsku_unit_sales,
       a.pfsku_discount_price,
       a.pfsku_page_price,
       a.pfsku_value_sales,
       a.pfsku_discountprice_value_sales,
       a.pfsku_pageprice_value_sales,
       a.sku_unit_sales,
       IFNULL(IF(a.sku_num = 1, a.sku_discount_price_tmp, b.sku_discount_price_tmp), c.sku_price) AS sku_discount_price_tmp,
       a.sku_discount_price,
       IF(a.sku_num = 1, a.pfsku_value_sales, NULL) AS sku_value_sales,
       IF(a.sku_num = 1, 1, sku_value_ratio) AS sku_value_ratio,
       IF(a.sku_num = 1, '单品不拆', sku_value_ratio_src) AS sku_value_ratio_src,
       a.sku_value_ratio_fixed,
       a.sku_page_price,
       a.sku_pageprice_value_sales,
       a.sku_discountprice_value_sales,
       a.sku_volume_sales,
       a.sku_pack_sales,
       a.fix_count_ratio,
       a.etl_updated_at,
       f.product_listing_time,
       a.std_category_1,
       a.std_category_2,
       a.std_category_3,
       a.std_category_4,
       a.std_category_5,
       a.std_category_6
       {month_field}
FROM t_staging a
LEFT JOIN t_sku_price_1 b ON a.market = b.f_market
AND a.platform = b.f_platform
AND a.month_dt = b.f_month_dt
AND a.std_sku_name = b.f_std_sku_name
LEFT JOIN {{ std_sku_fixed_price }} c ON a.platform = c.platform
AND a.month_dt = c.month_dt
AND a.std_sku_name = c.std_sku_name
LEFT JOIN {{ z_shop_name }} d ON a.platform =d.platform
AND a.market = d.market
AND a.shop_id = d.shop_id 
AND a.month_dt = d.month_dt
LEFT JOIN {{ std_brand_group_unique }} e ON a.std_brand_name = e.std_brand_name_ori
LEFT JOIN t_product_listing_time f ON lower(a.platform) = lower(f.listing_platform)
AND a.market = f.listing_market
AND IF(LOWER(a.platform) = 'amazon', a.pfsku_id, a.item_id) = f.listing_key
        """
    df = spark_sql_with_log(spark, query)
    return df


def loader(spark, df: DataFrame):
    try:
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
            repartition_num=PARTITION_NUM,
        )
    except Exception as e:
        # 兼容部分环境下 spark_util.insert 读取表结构列名为 col_name/name 导致的 KeyError('column_name')
        if 'column_name' in str(e) or 'col_name' in str(e):
            # 确保目标表存在
            spark.sql(DDL)
            # 动态分区设置
            spark.sql('set hive.exec.dynamic.partition=true')
            spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')
            # 列顺序按DDL重排，并追加分区列
            ordered_cols = QUERY_COLUMNS + ['month_dt']
            df_reordered = df.select(*ordered_cols)
            # 可选压缩设置
            if COMPRESSION:
                spark.sql(f"set parquet.compression={COMPRESSION}")
            # 重分区以控制小文件数量
            df_to_write = df_reordered.repartition(PARTITION_NUM, 'month_dt')
            # 插入（覆盖当期分区）
            df_to_write.write.mode('overwrite').format('hive').insertInto(f'{TARGET_DB}.{TARGET_TABLE}', overwrite=True)
        else:
            raise



def main():
    init_logging()
    impala = new_impala_connector(emr=True)
    spark = (SparkSession.builder.appName("rearc.{{dag_name}}.{{job_name}}.{{execution_date}}")
             .enableHiveSupport().getOrCreate())
    spark.sql(f'set spark.sql.shuffle.partitions={PARTITION_NUM}')
    spark.sql(f'set spark.sql.autoBroadcastJoinThreshold=-1')
    spark.sql(f'set spark.sql.broadcastTimeout=600')
    
    # 刷新表元数据，确保Spark能正确读取表结构（如果表存在）
    try:
        spark.sql(f'REFRESH TABLE {TARGET_DB}.{TARGET_TABLE}')
    except Exception:
        # 表不存在时忽略，create_table_ddl会自动创建
        pass

    df = dumper(spark)
    loader(spark, df)
    
    spark.stop()
    impala.execute(f'REFRESH {TARGET_DB}.{TARGET_TABLE} PARTITION({month_partition})')
    impala.execute(drop_incremental_stats)
    impala.execute(f'COMPUTE INCREMENTAL STATS {TARGET_DB}.{TARGET_TABLE} PARTITION({month_partition})')
    

if __name__ == '__main__':
    exit(main())