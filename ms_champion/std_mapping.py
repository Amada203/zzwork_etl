import collections
import copy
import json
from pyspark.sql import SparkSession
from pigeon.connector import new_impala_connector
from ymrbdt.attributes import read_rule_set

spark = SparkSession.builder.appName("oneflow.{{dag_name}}.{{job_name}}").enableHiveSupport().getOrCreate()
impala = new_impala_connector()

update_mode = '{{ update_mode }}'
month_dt = '{{ month_dt }}'
project_start_month = '{{ project_start_month }}'

source_table = '{{ std_mapping_origin }}'
result_table = '{{ std_mapping }}'

# 增量更新 或者 全量更新
if update_mode == 'incremental':
    month_field = ''
    month_condition = f'm.month_dt = "{month_dt}"'
    month_partition = f'month_dt = "{month_dt}"'
    refresh_table =  f'{{ refresh }} {result_table}  partition(month_dt="{month_dt}");'
    drop_incremental_stats = f'DROP INCREMENTAL STATS {result_table} PARTITION(month_dt="{month_dt}");'
    compute_incremental_stats = f'compute incremental stats {result_table}  partition(month_dt="{month_dt}");'
elif update_mode == 'full':
    month_field = ',b.month_dt'
    month_condition = '1=1'
    month_partition = 'month_dt'
    refresh_table =  f'{{ refresh }} {result_table};'
    drop_incremental_stats = f'DROP INCREMENTAL STATS {result_table} PARTITION(month_dt>="{project_start_month}");'
    compute_incremental_stats = f'compute incremental stats {result_table};'
else:
    raise ValueError(f'update_mode="{update_mode}" is invalid !!!')

rule_set_dict = {
    'std_category_1_tmp': (104769, read_rule_set(104769)),
    'fix_shop': (104732, read_rule_set(104732)),
    'fix_channel': (104733, read_rule_set(104733)),
    'fix_spu_name': (104736, read_rule_set(104736)),
    'fix_brand_name': (104735, read_rule_set(104735)),
    'fix_brand_name2': (104741, read_rule_set(104741)),
    'fix_brand_name3': (104744, read_rule_set(104744)),
    'fix_manufacturer': (104734, read_rule_set(104734)),
    'fix_category2': (104747, read_rule_set(104747)),
    'fix_category2_2': (104748, read_rule_set(104748)),
    'fix_category3': (104749, read_rule_set(104749)),
    'fix_lifestage': (104737, read_rule_set(104737)),
    'fix_lifestage2': (104742, read_rule_set(104742)),
    'fix_breed2': (104743, read_rule_set(104743)),
    'fix_body_type2': (104745, read_rule_set(104745)),
    'fix_country2': (104746, read_rule_set(104746)),
    'fix_is_natural2': (104750, read_rule_set(104750)),
    'fix_is_grain_free2': (104751, read_rule_set(104751)),
    'fix_freeze_dried_type2': (104752, read_rule_set(104752)),
    'fix_benefit2': (104753, read_rule_set(104753)),
    'fix_flavor2': (104754, read_rule_set(104754)),
    'fix_is_diet2': (104755, read_rule_set(104755)),
    'fix_is_fresh_meat2': (104756, read_rule_set(104756)),
    'fix_is_single_meat2': (104757, read_rule_set(104757)),
    'fix_is_freeze_dried2': (104758, read_rule_set(104758)),
    'fix_is_import2': (104759, read_rule_set(104759)),
    'fix_package_weight': (104760, read_rule_set(104760)),
    'is_excluded': (104738, read_rule_set(104738)),
    'sku_value_ratio': (104768, read_rule_set(104768)),
    
}

def get_mapping_result_and_src(row, rule_set_key):
    rule_set_id, rule_set = rule_set_dict[rule_set_key]
    rv = rule_set.transform(row)[0]
    value_dct = {}
    for col in rule_set.result_column:
        # 如果规则集返回 None、'NULL' 或空字符串，保留原值
        rule_value = rv.get(col)
        if rule_value not in [None, 'NULL', '']:
            value_dct[col] = rule_value
        else:
            value_dct[col] = row.get(col)
    if rv.get('__matched_rule') == '__DEFAULT__':
        priority = '_DEFAULT__'
    else:
        try:
            priority = json.loads(rv['__matched_rule'])['priority']
        except:
            try:
                priority = json.loads(rv['__matched_rule'][0])['priority']
            except:
                priority = '_DEFAULT__DN'
    src = f'{rule_set_id},{priority}'
    return value_dct, src


def transform(row):
    row.setdefault('std_category_1_tmp', None)
    row.setdefault('std_category_1', None)
    row.setdefault('std_category_2', None)
    row.setdefault('std_category_3', None)

    # std_category_1_tmp
    value, src = get_mapping_result_and_src(row, 'std_category_1_tmp')
    if 'std_category_1_tmp' in value and value['std_category_1_tmp']:
        row['std_category_1_tmp'] = value['std_category_1_tmp']
    # shop_id, shop_name
    value, src = get_mapping_result_and_src(row, 'fix_shop')
    if 'fix_shop_id' in value and value['fix_shop_id']:
        row['shop_id'] = value['fix_shop_id']
        row['shop_name'] = value['fix_shop_name']
    # std_spu_name - 第一步：规则清洗
    value, src = get_mapping_result_and_src(row, 'fix_spu_name')
    if 'fix_spu_name' in value and value['fix_spu_name']:
        row['std_spu_name'] = value['fix_spu_name']
        row['std_spu_name_src'] = src
    
    # 缓存fix_brand_name规则集结果，避免重复调用
    fix_brand_name_value, fix_brand_name_src = get_mapping_result_and_src(row, 'fix_brand_name')
    
    # std_brand_name 多级
    if 'std_brand_name' in fix_brand_name_value and fix_brand_name_value['std_brand_name']:
        row['std_brand_name'] = fix_brand_name_value['std_brand_name']
        row['std_brand_name_src'] = fix_brand_name_src
    else:
        # value2, src2 = get_mapping_result_and_src(row, 'fix_brand_name2')
        # if value2.get('fix_brand'):
        #     row['std_brand_name'] = value2['fix_brand']
        #     row['std_brand_name_src'] = src2
        if row.get('platform') in ('Tmall', 'Taobao') and row.get('pn_brandname_new'):
            row['std_brand_name'] = row.get('pn_brandname_new')
            row['std_brand_name_src'] = '{{ pn_monthly_sales_discount }}'
        elif row.get('platform') == 'JD' and row.get('rc_brandname'):
            row['std_brand_name'] = row.get('rc_brandname')
            row['std_brand_name_src'] = '{{ rc_monthly_sales_discount }}'
        else:
            value3, src3 = get_mapping_result_and_src(row, 'fix_brand_name3')
            if 'new_std_brand_name' in value3 and value3['new_std_brand_name']:
                row['std_brand_name'] = value3['new_std_brand_name']
                row['std_brand_name_src'] = src3
        # 先过完pn rn fix_brand_name3，再过fix_brand_name2，这样fix_brand_name2优先级也是高于pn rn fix_brand_name3，且pn rn fix_brand_name2调用的std_brand_name是经过pn rn fix_brand_name3后的
        value2, src2 = get_mapping_result_and_src(row, 'fix_brand_name2')
        if 'fix_brand' in value2 and value2['fix_brand']:
            row['std_brand_name'] = value2['fix_brand']
            row['std_brand_name_src'] = src2
            
        
    # std_manufacturer
    value, src = get_mapping_result_and_src(row, 'fix_manufacturer')
    if 'std_manufacturer' in value and value['std_manufacturer']:
        row['std_manufacturer'] = value['std_manufacturer']
        row['std_manufacturer_src'] = src
    
    # std_category_2 多级优先级 - 使用缓存的fix_brand_name结果
    if 'std_category_2' in fix_brand_name_value and fix_brand_name_value['std_category_2']:
        row['std_category_2'] = fix_brand_name_value['std_category_2']
        row['std_category_2_src'] = fix_brand_name_src
    else:
        # value2, src2 = get_mapping_result_and_src(row, 'fix_category2')
        # if 'fix_std_category_2' in value2 and value2['fix_std_category_2']:
        #     row['std_category_2'] = value2['fix_std_category_2']
        #     row['std_category_2_src'] = src2
        if row.get('platform') in ('Tmall', 'Taobao') and row.get('pn_sub_categoryname'):
            sub_categoryname = row.get('pn_sub_categoryname')
            row['std_category_2'] = {
                'Dog Dry': '狗干粮', 'Dog Wet': '狗湿粮', 'Dog C&T': '狗零食',
                'Cat Dry': '猫干粮', 'Cat Wet': '猫湿粮', 'Cat C&T': '猫零食'
            }.get(sub_categoryname)
            row['std_category_2_src'] = '{{ pn_monthly_sales_discount }}'
        elif row.get('platform') == 'JD' and row.get('rc_categoryname_revised'):
            categoryname_revised = row.get('rc_categoryname_revised')
            row['std_category_2'] = {
                'Dog Dry': '狗干粮', 'Dog Wet': '狗湿粮', 'Dog C&T': '狗零食',
                'Cat Dry': '猫干粮', 'Cat Wet': '猫湿粮', 'Cat C&T': '猫零食'
            }.get(categoryname_revised)
            row['std_category_2_src'] = '{{ rc_monthly_sales_discount }}'
        else:
            value3, src3 = get_mapping_result_and_src(row, 'fix_category2_2')
            if 'std_category_2' in value3 and value3['std_category_2']:
                row['std_category_2'] = value3['std_category_2']
                row['std_category_2_src'] = src3
        # 先过完pn rn fix_category2_2，再过fix_category2，这样fix_category2优先级也是高于pn rn fix_category2_2，且pn rn fix_category2_2调用的std_category_2是经过pn rn fix_category2_2后的
        value2, src2 = get_mapping_result_and_src(row, 'fix_category2')
        if 'fix_std_category_2' in value2 and value2['fix_std_category_2']:
            row['std_category_2'] = value2['fix_std_category_2']
            row['std_category_2_src'] = src2
    # std_category_1
    if row.get('std_category_2') in ('猫干粮','猫湿粮','猫零食'):
        row['std_category_1'] = '猫食品'
    elif row.get('std_category_2') in ('狗干粮','狗湿粮','狗零食'):
        row['std_category_1'] = '狗食品'
    
    # std_category_3 多级优先级 - 使用缓存的fix_brand_name结果
    if 'std_category_3' in fix_brand_name_value and fix_brand_name_value['std_category_3']:
        row['std_category_3'] = fix_brand_name_value['std_category_3']
        row['std_category_3_src'] = fix_brand_name_src
    else:
        value2, src2 = get_mapping_result_and_src(row, 'fix_category3')
        if 'std_category_3' in value2 and value2['std_category_3']:
            row['std_category_3'] = value2['std_category_3']
            row['std_category_3_src'] = src2    
    
    # 缓存fix_lifestage规则集结果，避免重复调用
    fix_lifestage_value, fix_lifestage_src = get_mapping_result_and_src(row, 'fix_lifestage')
    
    # lifestage
    if 'fix_lifestage' in fix_lifestage_value and fix_lifestage_value['fix_lifestage'] not in [None, 'NULL', '']:
        row['lifestage'] = fix_lifestage_value['fix_lifestage']
        row['lifestage_src'] = fix_lifestage_src
    else:
        value2, src2 = get_mapping_result_and_src(row, 'fix_lifestage2')
        if 'fix_lifestage' in value2 and value2['fix_lifestage'] not in [None, 'NULL', '']:
            row['lifestage'] = value2['fix_lifestage']
            row['lifestage_src'] = src2

    # breed - 使用缓存的fix_lifestage结果
    if 'fix_breed' in fix_lifestage_value and fix_lifestage_value['fix_breed'] not in [None, 'NULL', '']:
        row['breed'] = fix_lifestage_value['fix_breed']
        row['breed_src'] = fix_lifestage_src
    else:
        value2, src2 = get_mapping_result_and_src(row, 'fix_breed2')
        if 'fix_breed' in value2 and value2['fix_breed'] not in [None, 'NULL', '']:
            row['breed'] = value2['fix_breed']
            row['breed_src'] = src2
    # body_type - 使用缓存的fix_lifestage结果
    if 'fix_body_type' in fix_lifestage_value and fix_lifestage_value['fix_body_type'] not in [None, 'NULL', '']:
        row['body_type'] = fix_lifestage_value['fix_body_type']
        row['body_type_src'] = fix_lifestage_src
    else:
        value2, src2 = get_mapping_result_and_src(row, 'fix_body_type2')
        if 'fix_body_type' in value2 and value2['fix_body_type'] not in [None, 'NULL', '']:
            row['body_type'] = value2['fix_body_type']
            row['body_type_src'] = src2
    # country_of_origin - 使用缓存的fix_lifestage结果
    if 'fix_country_of_origin' in fix_lifestage_value and fix_lifestage_value['fix_country_of_origin'] not in [None, 'NULL', '']:
        row['country_of_origin'] = fix_lifestage_value['fix_country_of_origin']
        row['country_of_origin_src'] = fix_lifestage_src
    else:
        value2, src2 = get_mapping_result_and_src(row, 'fix_country2')
        if 'fix_country_of_origin' in value2 and value2['fix_country_of_origin'] not in [None, 'NULL', '']:
            row['country_of_origin'] = value2['fix_country_of_origin']
            row['country_of_origin_src'] = src2
    # is_natural - 使用缓存的fix_lifestage结果
    if 'fix_is_natural' in fix_lifestage_value and fix_lifestage_value['fix_is_natural'] not in [None, 'NULL', '']:
        row['is_natural'] = int(fix_lifestage_value['fix_is_natural'])
        row['is_natural_src'] = fix_lifestage_src
    else:
        value2, src2 = get_mapping_result_and_src(row, 'fix_is_natural2')
        if 'fix_is_natural' in value2 and value2['fix_is_natural'] not in [None, 'NULL', '']:
            row['is_natural'] = int(value2['fix_is_natural'])
            row['is_natural_src'] = src2
    # is_grain_free - 使用缓存的fix_lifestage结果
    if 'fix_is_grain_free' in fix_lifestage_value and fix_lifestage_value['fix_is_grain_free'] not in [None, 'NULL', '']:
        row['is_grain_free'] = int(fix_lifestage_value['fix_is_grain_free'])
        row['is_grain_free_src'] = fix_lifestage_src
    else:
        value2, src2 = get_mapping_result_and_src(row, 'fix_is_grain_free2')
        if 'fix_is_grain_free' in value2 and value2['fix_is_grain_free'] not in [None, 'NULL', '']:
            row['is_grain_free'] = int(value2['fix_is_grain_free'])
            row['is_grain_free_src'] = src2
    # freeze_dried_type - 使用缓存的fix_lifestage结果
    if 'fix_freeze_dried_type' in fix_lifestage_value and fix_lifestage_value['fix_freeze_dried_type'] not in [None, 'NULL', '']:
        row['freeze_dried_type'] = fix_lifestage_value['fix_freeze_dried_type']
        row['freeze_dried_type_src'] = fix_lifestage_src
    else:
        value2, src2 = get_mapping_result_and_src(row, 'fix_freeze_dried_type2')
        if 'fix_freeze_dried_type' in value2 and value2['fix_freeze_dried_type'] not in [None, 'NULL', '']:
            row['freeze_dried_type'] = value2['fix_freeze_dried_type']
            row['freeze_dried_type_src'] = src2
    # benefit - 使用缓存的fix_lifestage结果
    if 'fix_benefit' in fix_lifestage_value and fix_lifestage_value['fix_benefit'] not in [None, 'NULL', '']:
        row['benefit'] = fix_lifestage_value['fix_benefit']
        row['benefit_src'] = fix_lifestage_src
    else:
        value2, src2 = get_mapping_result_and_src(row, 'fix_benefit2')
        if 'fix_benefit' in value2 and value2['fix_benefit'] not in [None, 'NULL', '']:
            row['benefit'] = value2['fix_benefit']
            row['benefit_src'] = src2
    # flavor - 使用缓存的fix_lifestage结果
    if 'fix_flavor' in fix_lifestage_value and fix_lifestage_value['fix_flavor'] not in [None, 'NULL', '']:
        row['flavor'] = fix_lifestage_value['fix_flavor']
        row['flavor_src'] = fix_lifestage_src
    else:
        value2, src2 = get_mapping_result_and_src(row, 'fix_flavor2')
        if 'fix_flavor' in value2 and value2['fix_flavor'] not in [None, 'NULL', '']:
            row['flavor'] = value2['fix_flavor']
            row['flavor_src'] = src2
    # is_diet - 使用缓存的fix_lifestage结果
    if 'fix_is_diet' in fix_lifestage_value and fix_lifestage_value['fix_is_diet'] not in [None, 'NULL', '']:
        row['is_diet'] = int(fix_lifestage_value['fix_is_diet'])
        row['is_diet_src'] = fix_lifestage_src
    else:
        value2, src2 = get_mapping_result_and_src(row, 'fix_is_diet2')
        if 'fix_is_diet' in value2 and value2['fix_is_diet'] not in [None, 'NULL', '']:
            row['is_diet'] = int(value2['fix_is_diet'])
            row['is_diet_src'] = src2
    # is_fresh_meat - 使用缓存的fix_lifestage结果
    if 'fix_is_fresh_meat' in fix_lifestage_value and fix_lifestage_value['fix_is_fresh_meat'] not in [None, 'NULL', '']:
        row['is_fresh_meat'] = int(fix_lifestage_value['fix_is_fresh_meat'])
        row['is_fresh_meat_src'] = fix_lifestage_src
    else:
        value2, src2 = get_mapping_result_and_src(row, 'fix_is_fresh_meat2')
        if 'fix_is_fresh_meat' in value2 and value2['fix_is_fresh_meat'] not in [None, 'NULL', '']:
            row['is_fresh_meat'] = int(value2['fix_is_fresh_meat'])
            row['is_fresh_meat_src'] = src2
    # is_single_meat - 使用缓存的fix_lifestage结果
    if 'fix_is_single_meat' in fix_lifestage_value and fix_lifestage_value['fix_is_single_meat'] not in [None, 'NULL', '']:
        row['is_single_meat'] = int(fix_lifestage_value['fix_is_single_meat'])
        row['is_single_meat_src'] = fix_lifestage_src
    else:
        value2, src2 = get_mapping_result_and_src(row, 'fix_is_single_meat2')
        if 'fix_is_single_meat' in value2 and value2['fix_is_single_meat'] not in [None, 'NULL', '']:
            row['is_single_meat'] = int(value2['fix_is_single_meat'])
            row['is_single_meat_src'] = src2
    # is_freeze_dried - 使用缓存的fix_lifestage结果
    if 'fix_is_freeze_dried' in fix_lifestage_value and fix_lifestage_value['fix_is_freeze_dried'] not in [None, 'NULL', '']:
        row['is_freeze_dried'] = int(fix_lifestage_value['fix_is_freeze_dried'])
        row['is_freeze_dried_src'] = fix_lifestage_src
    else:
        value2, src2 = get_mapping_result_and_src(row, 'fix_is_freeze_dried2')
        if 'fix_is_freeze_dried' in value2 and value2['fix_is_freeze_dried'] not in [None, 'NULL', '']:
            row['is_freeze_dried'] = int(value2['fix_is_freeze_dried'])
            row['is_freeze_dried_src'] = src2
    # is_import - 使用缓存的fix_lifestage结果
    if 'fix_is_import' in fix_lifestage_value and fix_lifestage_value['fix_is_import'] not in [None, 'NULL', '']:
        row['is_import'] = int(fix_lifestage_value['fix_is_import'])
        row['is_import_src'] = fix_lifestage_src
    else:
        value2, src2 = get_mapping_result_and_src(row, 'fix_is_import2')
        if 'fix_is_import' in value2 and value2['fix_is_import'] not in [None, 'NULL', '']:
            row['is_import'] = int(value2['fix_is_import'])
            row['is_import_src'] = src2
    
    # package, weight,total_weight 
    value, src = get_mapping_result_and_src(row, 'fix_package_weight')
    if ('fix_package' in value and value['fix_package'] not in [None, 'NULL', '']) or ('fix_weight' in value and value['fix_weight'] not in [None, 'NULL', '']):
        # if row.get('total_weight_src') != '{{ upstream_monthly_sales_wide }}':
        if 'fix_package' in value and value['fix_package'] not in [None, 'NULL', '']:
            row['package'] = int(value['fix_package'])
            row['package_src'] = src
        if 'fix_weight' in value and value['fix_weight'] not in [None, 'NULL', '']:
            row['weight'] = float(value['fix_weight'])
            row['weight_src'] = src
        if row.get('package') is not None and row.get('weight') is not None:
            row['total_weight'] = float(row['package']) * float(row['weight'])
            row['total_weight_src'] = src
    
    # is_excluded
    value, src = get_mapping_result_and_src(row, 'is_excluded')
    if 'is_excluded' in value and value['is_excluded'] is not None:
        row['is_excluded'] = int(value['is_excluded'])
    # sku_value_ratio 优先rule 104768
    value, src = get_mapping_result_and_src(row, 'sku_value_ratio')
    if 'sku_value_ratio' in value and value['sku_value_ratio'] is not None:
        row['sku_value_ratio'] = float(value['sku_value_ratio'])
        row['sku_value_ratio_src'] = src
    # 在transform处理后计算标准化品牌名，用于后续JOIN
    if row.get('std_brand_name'):
        import re
        # 移除特殊字符和空格，用于标准化比较
        normalized = re.sub(r'[！!。，,、\s]', '', row['std_brand_name'].lower())
        row['std_brand_name_normalized'] = normalized
    # channel, ka_group, ka_name --20250722吴棉滨修改，依赖最终的std_brand_name
    value, src = get_mapping_result_and_src(row, 'fix_channel')
    if 'channel' in value and value['channel']:
        row['channel'] = value['channel']
        row['ka_group'] = value['ka_group']
        row['ka_name'] = value['ka_name']
        row['ka_name_src'] = src
    
    # std_spu_name - 第二步：品牌限制范围检查
    # 这里需要连表检查，但由于在transform函数中无法直接执行SQL，
  
    # std_sku_name - 依赖最终的std_spu_name
    # 注释：原有逻辑已移至SQL层面处理，依赖第二步清洗后的std_spu_name
    
    return row

def _spark_row_to_ordereddict(row):
    return collections.OrderedDict(zip(row.__fields__, row))

def apply_transformation(rows):
    for row in rows:
        yield transform(_spark_row_to_ordereddict(row))
        
def safe_int(s):
    if s is None:
        return s
    return int(s)  


def safe_float(s):
    if s is None:
        return s
    return float(s)


def safe_round(f):
    if f is None:
        return f
    return round(f)

SCHEMA = '''
  unique_id                        bigint,
  platform                         string,
  category_id                      bigint,
  category_name                    string,
  category_1                       string,
  category_2                       string,
  category_3                       string,
  shop_id                          bigint,
  shop_name                        string,
  channel                          string,
  ka_group                         string,
  ka_name                          string,
  ka_name_src                      string,
  brand_id                         string,
  brand_name                       string,
  item_id                          bigint,
  item_url                         string,
  item_title                       string,
  item_image                       string,
  is_multi_pfsku                   string,
  pfsku_id                         bigint,
  pfsku_url                        string,
  pfsku_title                      string,
  pfsku_image                      string,
  is_bundle                        int,
  sku_src                          string,
  sku_title                        string,
  sku_no                           int,
  sku_num                          int,
  sku_value_ratio	               double,
  sku_value_ratio_src              string,
  quantity                         bigint,
  std_brand_name                   string,
  std_brand_name_src               string,
  std_brand_name_normalized        string,
  std_spu_name                     string,
  std_spu_name_src                 string,
  std_sku_name                     string,
  std_manufacturer                 string,
  std_manufacturer_src             string,
  std_category_1_tmp               string,
  std_category_1                   string,
  std_category_2                   string,
  std_category_2_src               string,
  std_category_3                   string,
  std_category_3_src               string,
  lifestage                        string,
  breed                            string,
  body_type                        string,
  country_of_origin                string,
  is_natural                       int,
  is_grain_free                    int,
  freeze_dried_type                string,
  benefit                          string,
  flavor                           string,
  is_diet                          int,
  is_fresh_meat                    int,
  is_single_meat                   int,
  is_freeze_dried                  int,
  is_import                        int,
  lifestage_src                    string,
  breed_src                        string,
  body_type_src                    string,
  country_of_origin_src            string,
  is_natural_src                   string,
  is_grain_free_src                string,
  freeze_dried_type_src            string,
  benefit_src                      string,
  flavor_src                       string,
  is_diet_src                      string,
  is_fresh_meat_src                string,
  is_single_meat_src               string,
  is_freeze_dried_src              string,
  is_import_src                    string,
  package                          int,
  weight                           double,
  total_weight                     double,
  package_src                      string,
  weight_src                       string,
  total_weight_src                 string,  
  is_excluded                      int,
  tags_country                     string,
  etl_updated_at                   string,
  month_dt                         string
'''

DDL = f'''
CREATE TABLE IF NOT EXISTS { result_table }(
  unique_id                        bigint,
  platform                         string,
  category_id                      bigint,
  category_name                    string,
  category_1                       string,
  category_2                       string,
  category_3                       string,
  shop_id                          bigint,
  shop_name                        string,
  channel                          string,
  ka_group                         string,
  ka_name                          string,
  ka_name_src                      string,
  brand_id                         string,
  brand_name                       string,
  item_id                          bigint,
  item_url                         string,
  item_title                       string,
  item_image                       string,
  is_multi_pfsku                   string,
  pfsku_id                         bigint,
  pfsku_url                        string,
  pfsku_title                      string,
  pfsku_image                      string,
  is_bundle                        int,
  sku_src                          string,
  sku_title                        string,
  sku_no                           int,
  sku_num                          int,
  sku_value_ratio	               double,
  sku_value_ratio_src              string,
  quantity                         bigint,
  std_brand_name                   string,
  std_brand_name_src               string,
  std_spu_name                     string,
  std_spu_name_src                 string,
  std_sku_name                     string,
  std_manufacturer                 string,
  std_manufacturer_src             string,
  std_category_1_tmp               string,
  std_category_1                   string,
  std_category_2                   string,
  std_category_2_src               string,
  std_category_3                   string,
  std_category_3_src               string,
  lifestage                        string,
  breed                            string,
  body_type                        string,
  country_of_origin                string,
  is_natural                       int,
  is_grain_free                    int,
  freeze_dried_type                string,
  benefit                          string,
  flavor                           string,
  is_diet                          int,
  is_fresh_meat                    int,
  is_single_meat                   int,
  is_freeze_dried                  int,
  is_import                        int,
  lifestage_src                    string,
  breed_src                        string,
  body_type_src                    string,
  country_of_origin_src            string,
  is_natural_src                   string,
  is_grain_free_src                string,
  freeze_dried_type_src            string,
  benefit_src                      string,
  flavor_src                       string,
  is_diet_src                      string,
  is_fresh_meat_src                string,
  is_single_meat_src               string,
  is_freeze_dried_src              string,
  is_import_src                    string,
  package                          int,
  weight                           double,
  total_weight                     double,
  package_src                      string,
  weight_src                       string,  
  total_weight_src                 string,
  is_excluded                      int,
  tags_country                     string,
  etl_updated_at                   string
) PARTITIONED BY (month_dt string)
COMMENT '最细颗粒度宽表，含项目定制化清洗字段'
STORED AS PARQUET
'''

rdd = spark.sql(f'SELECT * FROM {source_table} m where {month_condition}').repartition(200).rdd.mapPartitions(apply_transformation)
spark.createDataFrame(rdd, SCHEMA).createOrReplaceTempView('tv_std_mapping')
spark.sql(DDL)
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql(f'''
WITH normalized_dimension AS (
    -- 预计算维度表的标准化品牌名
    SELECT 
        *,
        lower(REGEXP_REPLACE(std_brand_name, '[！!。，,、\\\\s]', '')) AS std_brand_name_normalized
    FROM ms_champion.category_brand_limit_dataprep
),
base_result AS (
SELECT
    /* +BROADCAST(l) */
    m.unique_id ,
    m.platform ,
    m.category_id ,
    m.category_name ,
    m.category_1 ,
    m.category_2 ,
    m.category_3 ,
    m.shop_id ,
    m.shop_name ,
    m.channel ,
    m.ka_group ,
    m.ka_name ,
    m.ka_name_src ,
    m.brand_id ,
    m.brand_name ,
    m.item_id ,
    m.item_url ,
    m.item_title ,
    m.item_image ,
    m.is_multi_pfsku ,
    m.pfsku_id ,
    m.pfsku_url ,
    m.pfsku_title ,
    m.pfsku_image ,
    m.is_bundle ,
    m.sku_src ,
    m.sku_title ,
    m.sku_no ,
    m.sku_num ,
    m.sku_value_ratio,
    m.sku_value_ratio_src,
    m.quantity ,
    m.std_brand_name ,
    m.std_brand_name_src ,
    -- std_spu_name 第二步清洗：品牌限制范围检查
    CASE 
        WHEN l.std_category_2 IS NULL -- 如果不在limit表，确保一定是机洗拼起来得到spu
        THEN CONCAT(
            COALESCE(m.std_brand_name, ''),
            COALESCE(m.lifestage, ''),
            COALESCE(m.std_category_3, '')
        )
        WHEN m.std_spu_name_src RLIKE '104736' 
             AND m.std_spu_name_src NOT RLIKE 'DEFAULT'
             and m.platform in ('JD','Tmall','Douyin')
        THEN m.std_spu_name
        WHEN l.std_category_2 IS NOT NULL 
             AND l.std_brand_name IS NOT NULL 
             AND m.sku_src <> '机器清洗'
        THEN m.std_spu_name
        ELSE CONCAT(
            COALESCE(m.std_brand_name, ''),
            COALESCE(m.lifestage, ''),
            COALESCE(m.std_category_3, '')
        )
    END AS std_spu_name,
    -- std_spu_name_src 更新来源
    CASE
        WHEN l.std_category_2 IS NULL -- 如果不在limit表，确保一定是机洗拼起来得到spu
        THEN 'brand_limit_rule'
        WHEN m.std_spu_name_src RLIKE '104736' 
             AND m.std_spu_name_src NOT RLIKE 'DEFAULT'
             and m.platform in ('JD','Tmall','Douyin')
        THEN m.std_spu_name_src
        WHEN l.std_category_2 IS NOT NULL 
             AND l.std_brand_name IS NOT NULL 
             AND m.sku_src <> '机器清洗'
        THEN m.std_spu_name_src
        ELSE 'brand_limit_rule'
    END AS std_spu_name_src,
    -- std_sku_name 依赖最终的std_spu_name
    CASE 
        WHEN l.std_category_2 IS NULL -- 如果不在limit表，确保一定是机洗拼起来得到spu
        THEN CASE 
                WHEN CONCAT(
                    COALESCE(m.std_brand_name, ''),
                    COALESCE(m.lifestage, ''),
                    COALESCE(m.std_category_3, '')
                ) IS NOT NULL and m.weight IS NOT NULL
                THEN CONCAT(
                    CONCAT(
                        COALESCE(m.std_brand_name, ''),
                        COALESCE(m.lifestage, ''),
                        COALESCE(m.std_category_3, '')
                    ),
                    CAST(CAST(m.weight AS DOUBLE) AS INT),
                    'g'
                )
                ELSE NULL
            END
        WHEN (m.std_spu_name_src RLIKE '104736' 
             AND m.std_spu_name_src NOT RLIKE 'DEFAULT'
             and m.platform in ('JD','Tmall','Douyin')
             AND m.std_spu_name IS NOT NULL)
             or
             (l.std_category_2 IS NOT NULL 
             AND l.std_brand_name IS NOT NULL 
             AND m.sku_src <> '机器清洗'
             AND m.std_spu_name IS NOT NULL)   
        THEN 
            -- 满足品牌限制条件，使用第一步清洗的std_spu_name
            CASE 
                WHEN m.std_spu_name IS NOT NULL and m.weight IS NOT NULL
                THEN CONCAT(m.std_spu_name, CAST(CAST(m.weight AS DOUBLE) AS INT), 'g')
                ELSE NULL
            END
        ELSE 
            CASE 
                WHEN CONCAT(
                    COALESCE(m.std_brand_name, ''),
                    COALESCE(m.lifestage, ''),
                    COALESCE(m.std_category_3, '')
                ) IS NOT NULL and m.weight IS NOT NULL
                THEN CONCAT(
                    CONCAT(
                        COALESCE(m.std_brand_name, ''),
                        COALESCE(m.lifestage, ''),
                        COALESCE(m.std_category_3, '')
                    ),
                    CAST(CAST(m.weight AS DOUBLE) AS INT),
                    'g'
                )
                ELSE NULL
            END
    END AS std_sku_name,
    m.std_manufacturer ,
    m.std_manufacturer_src ,
    m.std_category_1_tmp ,
    m.std_category_1 ,
    m.std_category_2 ,
    m.std_category_2_src ,
    m.std_category_3 ,
    m.std_category_3_src ,
    m.lifestage ,
    m.breed ,
    m.body_type ,
    m.country_of_origin ,
    m.is_natural ,
    m.is_grain_free ,
    m.freeze_dried_type ,
    m.benefit ,
    m.flavor ,
    m.is_diet ,
    m.is_fresh_meat ,
    m.is_single_meat ,
    m.is_freeze_dried ,
    m.is_import ,
    m.lifestage_src ,
    m.breed_src ,
    m.body_type_src ,
    m.country_of_origin_src ,
    m.is_natural_src ,
    m.is_grain_free_src ,
    m.freeze_dried_type_src ,
    m.benefit_src ,
    m.flavor_src ,
    m.is_diet_src ,
    m.is_fresh_meat_src ,
    m.is_single_meat_src ,
    m.is_freeze_dried_src ,
    m.is_import_src ,
    m.package ,
    m.weight ,
    m.total_weight ,
    m.package_src ,
    m.weight_src ,
    m.total_weight_src ,
    m.is_excluded ,
    m.tags_country ,
    m.etl_updated_at,
    m.month_dt
FROM tv_std_mapping m
LEFT JOIN normalized_dimension l ON m.month_dt BETWEEN l.start_time AND l.end_time AND m.std_category_2 = l.std_category_2
   -- 使用预计算的标准化字段进行JOIN，避免重复正则计算
   AND m.std_brand_name_normalized = l.std_brand_name_normalized
   AND lower(m.platform) = lower(l.platform)  
where {month_condition}
)
INSERT OVERWRITE TABLE {result_table} PARTITION({month_partition})
SELECT  
    /*+ BROADCAST(f),REPARTITION(30) */
    b.unique_id ,
    b.platform ,
    b.category_id ,
    b.category_name ,
    b.category_1 ,
    b.category_2 ,
    b.category_3 ,
    b.shop_id ,
    b.shop_name ,
    b.channel ,
    b.ka_group ,
    b.ka_name ,
    b.ka_name_src ,
    b.brand_id ,
    b.brand_name ,
    b.item_id ,
    b.item_url ,
    b.item_title ,
    b.item_image ,
    b.is_multi_pfsku ,
    b.pfsku_id ,
    b.pfsku_url ,
    b.pfsku_title ,
    b.pfsku_image ,
    b.is_bundle ,
    b.sku_src ,
    b.sku_title ,
    b.sku_no ,
    b.sku_num ,
    b.sku_value_ratio,
    b.sku_value_ratio_src,
    b.quantity ,
    b.std_brand_name ,
    b.std_brand_name_src ,
    -- std_spu_name 20250823基于final_spu_mapping_dataprep表的二次映射
    COALESCE(f.std_spu_name_map, b.std_spu_name) AS std_spu_name,
    -- std_spu_name_src 更新来源--20250823
    CASE WHEN f.std_spu_name_map IS NOT NULL THEN '{{final_spu_mapping_dataprep}}'
        ELSE b.std_spu_name_src
    END AS std_spu_name_src,
    -- std_sku_name 依赖最终的std_spu_name（包含二次映射结果）
    CASE 
        WHEN (f.std_spu_name_map IS NOT NULL OR b.std_spu_name IS NOT NULL) AND b.weight IS NOT NULL
        THEN CONCAT(
            COALESCE(f.std_spu_name_map, b.std_spu_name), 
            CAST(CAST(b.weight AS DOUBLE) AS INT), 
            'g')
        ELSE NULL
    END AS std_sku_name,
    b.std_manufacturer ,
    b.std_manufacturer_src ,
    b.std_category_1_tmp ,
    b.std_category_1 ,
    b.std_category_2 ,
    b.std_category_2_src ,
    b.std_category_3 ,
    b.std_category_3_src ,
    b.lifestage ,
    b.breed ,
    b.body_type ,
    b.country_of_origin ,
    b.is_natural ,
    b.is_grain_free ,
    b.freeze_dried_type ,
    b.benefit ,
    b.flavor ,
    b.is_diet ,
    b.is_fresh_meat ,
    b.is_single_meat ,
    b.is_freeze_dried ,
    b.is_import ,
    b.lifestage_src ,
    b.breed_src ,
    b.body_type_src ,
    b.country_of_origin_src ,
    b.is_natural_src ,
    b.is_grain_free_src ,
    b.freeze_dried_type_src ,
    b.benefit_src ,
    b.flavor_src ,
    b.is_diet_src ,
    b.is_fresh_meat_src ,
    b.is_single_meat_src ,
    b.is_freeze_dried_src ,
    b.is_import_src ,
    b.package ,
    b.weight ,
    b.total_weight ,
    b.package_src ,
    b.weight_src ,
    b.total_weight_src ,
    b.is_excluded ,
    b.tags_country ,
    date_format(now(), 'yyyy-MM-dd HH:mm:ss') AS etl_updated_at 
    {month_field}
FROM base_result b
LEFT JOIN {{final_spu_mapping_dataprep}} f ON b.std_spu_name = f.std_spu_name   
''')

spark.stop()

impala.execute(refresh_table) 
impala.execute(drop_incremental_stats)
impala.execute(compute_incremental_stats)