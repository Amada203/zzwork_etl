-- ==================== 版本信息 ====================
-- 版本: v1214
-- 说明: JD周度销量预测修正脚本
-- 功能: 基于月度销量数据对预测值进行修正，优先使用ms_aowei_cloud.monthly_sales_wide，其次使用jd.monthly_sales_origin
-- ==================== 版本信息结束 ====================

create table if not exists  {{ sales_prediction_fix }}(
            sku_id STRING ,
            category_id STRING ,
            category_name STRING ,
            comments_number BIGINT ,
            comments_score DOUBLE ,
            predicted_weekly_sales BIGINT ,
            comments_number_diff BIGINT ,
            days_diff DOUBLE, 
            origin_count BIGINT,
            weekly_count BIGINT,
            weekly_count_src   STRING ,
            dt STRING ,
            prediction_dt STRING 
            
        )
        PARTITIONED BY (next_week_dt STRING )
        STORED AS PARQUET;

insert overwrite table {{sales_prediction_fix}} partition(next_week_dt)
select
    a.sku_id,
    a.category_id,
    a.category_name,
    a.comments_number,
    a.comments_score,
    a.predicted_weekly_sales,
    -- 当days_diff=0时，comments_number_diff修正为0
    CASE WHEN a.days_diff = 0 THEN 0 ELSE a.comments_number_diff END as comments_number_diff,
    a.days_diff,
    -- origin_count: 优先使用c表，其次使用b表，保持月度数据
    COALESCE(
      CASE WHEN c.pfsku_unit_sales IS NOT NULL AND c.pfsku_unit_sales >= 0 THEN c.pfsku_unit_sales ELSE NULL END,
      CASE WHEN b.`count` > 0 THEN b.`count` ELSE NULL END
    ) as origin_count,
    
    CAST(
      CASE 
        -- 优先使用c表（ms_aowei_cloud.monthly_sales_wide）的数据，直接计算周化值，不进行修正
        WHEN c.pfsku_unit_sales IS NOT NULL AND c.pfsku_unit_sales >= 0 THEN 
          GREATEST(
            CAST(ROUND(c.pfsku_unit_sales * 7.0 / DAY(LAST_DAY(TO_DATE(c.month_dt))), 0) AS BIGINT),
            CASE WHEN c.pfsku_unit_sales > 0 THEN 1 ELSE 0 END
          )
        -- 其次使用b表（jd.monthly_sales_origin）的数据，进行修正逻辑
        WHEN b.`count` > 0 THEN 
          CASE 
            -- 低于下限（0.9倍基准值）时优先回补至基准周化值（允许按预测值放大至20倍上限）
            WHEN a.predicted_weekly_sales < CAST(ROUND(b.`count` * 7.0 / DAY(LAST_DAY(TO_DATE(b.month))) * 0.9, 0) AS BIGINT)
              THEN CASE 
                WHEN CAST(a.predicted_weekly_sales AS DOUBLE) * 21 < GREATEST(
                  CAST(ROUND(b.`count` * 7.0 / DAY(LAST_DAY(TO_DATE(b.month))), 0) AS BIGINT),
                  CASE WHEN b.`count` > 0 THEN 1 ELSE 0 END
                )
                  THEN CAST(a.predicted_weekly_sales AS DOUBLE) * 21
                ELSE GREATEST(
                  CAST(ROUND(b.`count` * 7.0 / DAY(LAST_DAY(TO_DATE(b.month))), 0) AS BIGINT),
                  CASE WHEN b.`count` > 0 THEN 1 ELSE 0 END
                )
              END
            -- 高于上限（10倍基准值）时优先压回到基准周化值的10倍（允许按预测值回落至0.8倍下限）
            WHEN a.predicted_weekly_sales > CAST(ROUND(b.`count` * 7.0 / DAY(LAST_DAY(TO_DATE(b.month))) * 10, 0) AS BIGINT)
              THEN CASE 
                WHEN CAST(a.predicted_weekly_sales AS DOUBLE) * 0.8 < CAST(ROUND(b.`count` * 7.0 / DAY(LAST_DAY(TO_DATE(b.month))) * 10, 0) AS BIGINT)
                  THEN CAST(a.predicted_weekly_sales AS DOUBLE) * 0.8
                ELSE CAST(ROUND(b.`count` * 7.0 / DAY(LAST_DAY(TO_DATE(b.month))) * 10, 0) AS BIGINT)
              END
            ELSE a.predicted_weekly_sales 
          END
        ELSE a.predicted_weekly_sales 
      END AS BIGINT
    ) AS weekly_count,


CASE 
  -- 优先使用c表（ms_aowei_cloud.monthly_sales_wide）的数据
  WHEN c.pfsku_unit_sales IS NOT NULL AND c.pfsku_unit_sales >= 0 THEN 
    'ms_aowei_cloud.monthly_sales_wide'
  -- 其次使用b表（jd.monthly_sales_origin）的数据
  WHEN b.`count` > 0 THEN 
    CASE 
      WHEN a.predicted_weekly_sales < CAST(ROUND(b.`count` * 7.0 / DAY(LAST_DAY(TO_DATE(b.month))) * 0.9, 0) AS BIGINT) THEN 
        CASE 
            WHEN CAST(ROUND(b.`count` * 7.0 / DAY(LAST_DAY(TO_DATE(b.month))), 0) AS BIGINT) < CAST(a.predicted_weekly_sales AS DOUBLE) * 21 
            THEN 'jd.monthly_sales_origin'
          ELSE 'sales_prediction'
        END
      WHEN a.predicted_weekly_sales > CAST(ROUND(b.`count` * 7.0 / DAY(LAST_DAY(TO_DATE(b.month))) * 10, 0) AS BIGINT) THEN 
        CASE 
            WHEN CAST(ROUND(b.`count` * 7.0 / DAY(LAST_DAY(TO_DATE(b.month))) * 10, 0) AS BIGINT) < CAST(a.predicted_weekly_sales AS DOUBLE) * 0.8 
            THEN 'jd.monthly_sales_origin'
          ELSE 'sales_prediction'
        END
      ELSE 'sales_prediction'
    END
  ELSE 'sales_prediction' 
    END AS weekly_count_src,
    a.dt,
    a.prediction_dt,
    a.next_week_dt 
from {{sales_prediction}} a  left join 
(select * from jd.monthly_sales_origin  where 
    -- 示例: next_week_dt='2025-12-01' -> '2025-11-30' -> 上一个月 -> '2025-10-01'
    month =  SUBSTRING(CAST(ADD_MONTHS(DATE_TRUNC('month', DATE_SUB(TO_DATE('{{ next_week_dt }}'), 1)), -1) AS STRING), 1, 10)
  )b on 
    a.sku_id = CAST(b.sku_id AS STRING)
  left JOIN
  (select * from ms_aowei_cloud.monthly_sales_wide   where 
    -- 示例: next_week_dt='2025-12-01' -> '2025-11-30' -> 上一个月 -> '2025-10-01'
    month_dt =  SUBSTRING(CAST(ADD_MONTHS(DATE_TRUNC('month', DATE_SUB(TO_DATE('{{ next_week_dt }}'), 1)), -1) AS STRING), 1, 10)
  )c on 
    a.sku_id = CAST(c.pfsku_id AS STRING)

where a.next_week_dt = '{{ next_week_dt }}' 
;

COMPUTE INCREMENTAL STATS {{ sales_prediction_fix }};