# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 11:43:26 2024

@author: deepu
"""


Json_input = {'metrics_clicks':30,'metrics_conversions':40,'metrics_cost_micros':35
                  ,'metrics_cost_per_conversion':55
                  ,'campaign_id':['16951841500','16951841500']}
            
dimensions=['campaign_id',	'customer_id',	'segments_product_aggregator_id',	
            'segments_product_item_id',	'segments_product_merchant_id',	'campaign_status',	
            'segments_ad_network_type',	'segments_date',	'segments_day_of_week',	'segments_device',	
            'segments_month',	'segments_product_bidding_category_level1',	
            'segments_product_bidding_category_level2',	'segments_product_bidding_category_level3',	
            'segments_product_bidding_category_level4',	'segments_product_bidding_category_level5',	
            'segments_product_brand',	'segments_product_channel',	'segments_product_channel_exclusivity',	
            'segments_product_condition',	'segments_product_country',	'segments_product_language'
            ,'segments_product_type_l1',	'segments_product_type_l2',	'segments_product_type_l3',
            'segments_product_type_l4',	'segments_product_type_l5',	'segments_quarter',	'segments_week',
            'segments_year']
metrics=['metrics_all_conversions',	'metrics_all_conversions_value'
         ,	'metrics_average_cpc',	'metrics_clicks',	'metrics_conversions'
         ,	'metrics_conversions_value',	'metrics_cost_micros',	'metrics_cost_per_all_conversions'
         ,	'metrics_cost_per_conversion',	'metrics_cross_device_conversions',	'metrics_impressions'
         ,	'metrics_value_per_all_conversions',	'metrics_value_per_conversion']


#  For splitting json keys into columns list
col_list=[]
for x in Json_input.keys():
    col_list.append(x)

# For splitting columns into metrics & dimensions
def intersection(lst1, lst2):
    return list(set(lst1) & set(lst2))
metrics=intersection(col_list,metrics)
target_dim_list=intersection(col_list,dimensions)
target_metrics_list = ['sum('+x+')'  for x in metrics]


# print(Json_input['campaign_id'])
# Where conditions
Where_conditions=[]

for x,y in Json_input.items():
    if type(y).__name__!='list':
        z=x+'>'+str(y)
        Where_conditions.append(z)  
    elif type(y).__name__=='str':
        z=x+' in( '+','.join(y)+')'
        Where_conditions.append(z)  


table_name = 'quantacusgrowthbot.Zalora_google_ads_southeast_asia1.p_ads_ShoppingProductStats_6387311528'
startdate = '2024-04-01'
enddate= '2024-04-03'
sql_data_sample = str("""select segments_product_item_id,{0},{1} 
                                  from {2} 
                                      where segments_date between {3} and {4} and {5}
                                          group by segments_product_item_id,{0};""").format(','.join(target_dim_list),','.join(target_metrics_list)
                                    ,table_name,startdate
                                    ,enddate,' and '.join(Where_conditions))
    
print("###"*20)
print (sql_data_sample)

