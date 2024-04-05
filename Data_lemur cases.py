# -*- coding: utf-8 -*-
"""
Created on Wed Mar 20 19:43:40 2024

@author: deepu
"""

# def func(a, b, *args, **kwargs):
#     print(a, b)
#     print(args)
#     print(kwargs)
# func(1, 3, 10, 20, Name = 'Tom', Salary = 60000)


data = [
    {'category': 'A', 'sub_category': 'X', 'value': 10},
    {'category': 'A', 'sub_category': 'Y', 'value': 20},
    {'category': 'B', 'sub_category': 'X', 'value': 15},
    {'category': 'B', 'sub_category': 'Y', 'value': 25},
    {'category': 'A', 'sub_category': 'X', 'value': 30},
]
aggregated_data = {}
for entry in data:
    cat=entry['category']
    sub_cat=entry['sub_category']
    value=entry['value']
    if (cat,sub_cat) not in aggregated_data:
        aggregated_data[(cat,sub_cat)]=value
    else:
        aggregated_data[(cat,sub_cat)]+=value
print(aggregated_data)
        
        