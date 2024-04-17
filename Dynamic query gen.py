# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 10:44:47 2024

@author: deepu
"""

# def build_dynamic_query(table, columns):
#     query = f"SELECT {', '.join(columns)} FROM {table};"
#     return query

# if __name__ == "__main__":
#     # Example text input
#     table_name = "Customers"
#     column_input = "CustomerID, FirstName, LastName"
    
#     # Split the column input into a list of columns
#     columns = [col.strip() for col in column_input.split(',')]
    
#     # Call the function to build the dynamic query
#     dynamic_query = build_dynamic_query(table_name, columns)
    
#     print("Dynamic SQL Query:")
#     print(dynamic_query)


def translate_time_filter_to_sql(filter_condition, field_name="segment_date"):
  """
  Translate time filters into SQL WHERE clause conditions.
  Assumes the use of SQL functions similar to PostgreSQL for date manipulations.
  :param filter_condition: The human-readable time filter condition (e.g., "this month").
  :param field_name: The name of the date field to apply the filter to.
  :return: SQL WHERE clause condition string.
  """
  base_condition = ""
  if filter_condition == "today":
    base_condition = f"{field_name} = CURRENT_DATE"
  elif filter_condition == "yesterday":
    base_condition = f"{field_name} = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)"
  elif filter_condition == "this week":
    base_condition = f"{field_name} >= DATE_SUB(DATE_TRUNC(current_date(), WEEK(MONDAY)), INTERVAL 0 WEEK)"
  elif filter_condition == "last week":
    base_condition = f"{field_name} >= DATE_SUB(DATE_TRUNC(current_date(), WEEK(MONDAY)), INTERVAL 1 WEEK) AND {field_name} < DATE_SUB(DATE_TRUNC(current_date(), WEEK(MONDAY)), INTERVAL 1 DAY)"
  elif filter_condition == "this month":
    base_condition = f"{field_name} >= DATE_TRUNC(CURRENT_DATE(), MONTH)"
  elif filter_condition == "last month":
    base_condition = f"{field_name} >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH) AND {field_name} < DATE_TRUNC(CURRENT_DATE(), MONTH)"
  elif filter_condition == "this quarter":
    base_condition = f"{field_name} >= DATE_TRUNC(CURRENT_DATE(), QUARTER)"
  elif filter_condition == "last quarter":
    base_condition = f"{field_name} >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 1 QUARTER) AND DATE_SUB(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 1 DAY)"
  elif filter_condition == "this year":
    base_condition = f"{field_name} >= DATE_TRUNC(CURRENT_DATE(), YEAR)"
  elif filter_condition == "last year":
    base_condition = f"{field_name} >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), YEAR), INTERVAL 1 YEAR) AND {field_name} < DATE_SUB(DATE_TRUNC(CURRENT_DATE(), YEAR), INTERVAL 1 DAY)"
  else:
    base_condition = f"{field_name} = {field_name} -- No matching time filter"
  return base_condition
def generate_base_where_clause(categorizations):
  """
  Generate the base WHERE clause dynamically based on the provided categorizations,
  excluding the condition related to a.table_name being in the inner query result.
  """
  base_conditions = [f"a.{cat} IS NOT NULL" for cat in categorizations if cat != 'table_name']
  return " AND ".join(base_conditions)
def generate_group_by_clause(categorizations):
  """
  Generate the GROUP BY clause based on the provided categorizations.
  :param categorizations: A list of fields on which to group the results.
  :return: A string representing the GROUP BY clause of an SQL query.
  """
  group_by_fields = ", ".join([f"a.{cat}" for cat in categorizations])
  return f"GROUP BY {group_by_fields}"
def generate_order_by_clause(categorizations):
  """
  Generate the ORDER BY clause based on the categorizations.
  """
  order_by_fields = ", ".join([f"a.{cat}" for cat in categorizations])
  return f"ORDER BY {order_by_fields}"
def generate_inner_query(categorizations, metric):
  """
  Generate the inner query dynamically based on the provided categorizations and metric.
  The metric determines the ordering in the inner query.
  """
  agg_func, field = metric.split(' of ')
  inner_select_group_by = ", ".join([f"b.{cat}" for cat in categorizations if cat != 'table_name'])
  inner_order_by = f"{agg_func.upper()}(b.{field}) DESC"
  inner_query = f"""
    SELECT b.table_name
    FROM ga4-bq-connector.at_dataset.at_p_basic_stats_all b
    WHERE {' AND '.join([f'b.{cat} IS NOT NULL' for cat in categorizations if cat != 'table_name'])}
    GROUP BY {inner_select_group_by}, b.table_name
    ORDER BY {inner_order_by}, b.table_name LIMIT 1
  """
  return inner_query.strip()
def parse_metric(metric, alias_index):
  """
  Parse the metric to determine the SQL aggregate function and field.
  Assign a unique alias based on the function, field, and index, ensuring correct format for sum, avg, max, and min.
  """
  agg_func, field = metric.split(' of ')
  if agg_func.lower() in ['sum', 'avg', 'max', 'min']:
    return f"{agg_func.upper()}(a.{field}) AS {agg_func}_{alias_index}"
  elif agg_func.lower() == 'median':
    # Median approximation using PERCENTILE_CONT as SQL standard does not define MEDIAN
    return f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY a.{field}) OVER () AS median_{alias_index}"
  else:
    return f"SUM(a.{field}) AS sum_{alias_index}" # Default case if no known aggregate function pattern is matched
def translate_filter_to_sql(filter_condition):
  """
  Translate a simple English filter condition into SQL.
  """
  field_name="segments_date"
  # Date range condition
  time_filters = ["today", "yesterday", "this week", "last week", "this month", "last month", "this quarter", "last quarter", "this year", "last year"]
  # Check if the filter condition is a recognized time filter
  if filter_condition in time_filters:
    return translate_time_filter_to_sql(filter_condition, field_name)
  # Existing logic for non-time filters
  elif 'between' in filter_condition:
    field, range_part = filter_condition.split(' between ')
    start, end = range_part.split(' and ')
    # Correcting the quote handling here
    return f"a.{field} BETWEEN {start} AND {end}"
  # Equality condition
  elif 'is' in filter_condition:
    field, value = filter_condition.split(' is ')
    return f"a.{field} = '{value}'"
  # Add more specific translations as needed
  return filter_condition
def map_categorizations(categorizations):
  """
  Map specific categorization keywords to their corresponding field names.
  :param categorizations: A list of categorization keywords.
  :return: A list of mapped field names.
  """
  mapping = {
    "daily": "segments_date",
    "weekly": "segments_week",
    "quarterly": "segments_quarter",
    "yearly": "segments_year"
  }
  # Map each categorization keyword to its corresponding field name, if applicable
  mapped_categorizations = [mapping.get(cat, cat) for cat in categorizations]
  return mapped_categorizations
def adjust_order_by_and_limit(categorizations, filters, metrics, order_by_clause):
  """
  Adjust the ORDER BY clause and append a LIMIT clause based on 'top n' or 'bottom n' specifications.
  """
  limit_clause = ""
  top_bottom_filter = next((f for f in filters if "top " in f or "bottom " in f), None)
  if top_bottom_filter:
    direction = "DESC" if "top " in top_bottom_filter else "ASC"
    limit_value = top_bottom_filter.split(" ")[1] # Assumes format 'top n' or 'bottom n'
    # Directly using metrics[0] to determine the aggregate function and field
    agg_func, field = metrics[0].split(' of ')
    agg_func = agg_func.upper() # Ensure aggregate function is in uppercase for SQL
    # Assuming the first metric's field name is to be used in the ORDER BY clause
    updated_order_by_clause = f"ORDER BY {agg_func}(a.{field}) {direction}, " + order_by_clause.partition('ORDER BY ')[2]
    limit_clause = f"LIMIT {limit_value}"
  else:
    updated_order_by_clause = order_by_clause
  return updated_order_by_clause, limit_clause


def dimension_related(categorizations):
    where=""
    col_list=""
    for i in range(0,len(categorizations)):
        if i==0:
            y="{} is Not null".format(categorizations[i])
            y1=categorizations[i]
            where=where+y
            col_list=col_list+y1
        else:
            z=" and {} is not null".format(categorizations[i])
            z1=",{}".format(categorizations[i])
            where=where+z
            col_list=col_list+z1
    return where,col_list

def metrics_related(metrics):
    metr_list_agg=""
    metr_list=""
    for i in range(0,len(metrics)):
        y=",sum({0}) as {1}".format(metrics[i],metrics[i])
        y1=",{}".format(metrics[i])
        metr_list_agg=metr_list_agg+y
        metr_list=metr_list+y1
    return metr_list_agg,metr_list


def generate_sql_query(categorizations, filters, metrics):
  where_condition,col_list=dimension_related(categorizations)
  categorizations = map_categorizations(categorizations)
  base_select = ", ".join([f"{cat}" for cat in categorizations])
  metrics_select,metr_list = metrics_related(metrics)
  print("------------------------------------------------")
  inner_query = f"""SELECT distinct b.table_name
    FROM ga4-bq-connector.at_dataset.at_p_basic_stats_all b
    WHERE {where_condition}"""
  base_where = generate_base_where_clause(categorizations)
  group_by_clause = generate_group_by_clause(categorizations)
  order_by_clause = generate_order_by_clause(categorizations)
  # Adjust the ORDER BY clause and add LIMIT for 'top n' or 'bottom n' specifications
  order_by_clause, limit_clause = adjust_order_by_and_limit(categorizations, filters, metrics, order_by_clause)
  # Adding the condition for a.table_name based on the inner query result
  complete_base_where = f"({where_condition}) AND a.table_name IN ({inner_query})"
  filters_where = " AND ".join([translate_filter_to_sql(filter_condition) for filter_condition in filters if not ("top " in filter_condition or "bottom " in filter_condition)])
  sql_query = f"""
                SELECT {col_list} {metrics_select}
                FROM ga4-bq-connector.at_dataset.at_p_basic_stats_all a
                WHERE {complete_base_where}
                AND {filters_where}
                group by {col_list}
                order by {col_list}{metr_list}
                {limit_clause}
                 """.strip()
  return sql_query

# sql_query = generate_sql_query(categorizations, filters, metrics)
# print(sql_query)
# print(sql_query)

x=input("Enter input values :")
words=x.split(" ")
print(words)
col_values=["Impressions","Clicks","Cost","Conversions","Conversion value"]

def intersection(lst1, lst2):
    return list(set(lst1) & set(lst2))


def generate_sql_query_route():
  categorizations=["Campaign_ID","segments_date"]
  filters=["this month"]
  metrics_output=intersection(words, col_values)
  metrics_mapping ={'Impressions': 'metrics_impressions'
                    , 'Clicks': 'metrics_clicks'
                    , 'Cost': 'metrics_cost_micros'
                    ,'Conversions': 'metrics_conversions'
                    ,'Conversion value':'metrics_conversions_value'}
  metrics=list(map(metrics_mapping.get,metrics_output))
  sql_query = generate_sql_query(categorizations, filters, metrics)
  print(sql_query)
  return sql_query

#########################################################
# Run the Flask application
if __name__ == '__main__':
    generate_sql_query_route()
############################################################