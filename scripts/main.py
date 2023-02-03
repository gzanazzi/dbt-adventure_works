from collections import defaultdict
from dataclasses import dataclass
import json
import sys
import os
import yaml
import openpyxl
import raw_layer
import curated_layer
import sources_yml_file
import functions



wbtd = openpyxl.load_workbook("/home/gzanazzi/adventure_works_playground/scripts/model_generation/tables_definitions.xlsx")

model_xls_source_tables_raw = {
        "sales_currency" : "dbt_adventure_works",
        "sales_currency_rate" : "dbt_adventure_works",
        "sales_customer" : "dbt_adventure_works",
        "sales_order_detail" : "dbt_adventure_works",
        "sales_order_header" : "dbt_adventure_works",
        "sales_person" : "dbt_adventure_works",
        "sales_territory" : "dbt_adventure_works"
    }

map_bq_names ={
    "LKP_ARTICLE_RECLASS_CHANGES" : "EXTBI_LKP_ARTICLE_RECLASS_CHANGES"
}

model_xls_source_tables_curated = {
        "sales_currency" : "dbt_adventure_works",
        "sales_currency_rate" : "dbt_adventure_works",
        "sales_customer" : "dbt_adventure_works",
        "sales_order_detail" : "dbt_adventure_works",
        "sales_order_header" : "dbt_adventure_works",
        "sales_person" : "dbt_adventure_works",
        "sales_territory" : "dbt_adventure_works"
    }

ops = 2

schema_dict=defaultdict(list)
table_dict=defaultdict(list)

for tab, schema in model_xls_source_tables_raw.items():
    #print(schema)
    schema_dict[schema]
    if tab in map_bq_names.keys():
        tab_bq = map_bq_names[tab]
    else:
        tab_bq = tab
    if schema in schema_dict and tab_bq not in schema_dict[schema]:
        schema_dict[schema].append(tab_bq)
print(json.dumps(schema_dict, indent = 4))


if ops == 1:   # WRITE YAML SOURCE FILE
    #print("chose option 1")
    tgt_yaml_file = r"models/adventure_works_playground_sources.yml"
    print("creo " + tgt_yaml_file)
    #print(json.dumps(schema_dict, indent=4))
    sources_yml_file.write_source_file(tgt_yaml_file,schema_dict)

if ops == 2:
    raw_layer.write_raw_layer(wbtd,model_xls_source_tables_raw,map_bq_names,0)

if ops == 3:
    curated_layer.write_curated_layer(wbtd,model_xls_source_tables_curated,0)


    #sources_yml_file.write_source_file(tgt_yaml_file, model_source_tables)

# ------------------------  write dbt source for dataset
# write_sources_files=False
# if write_sources_files:
#     dbt_version_file = r"2"
#     tgt_yaml_file = r"models/flash_sales_sources.yml"
#     print("build source yml file: " + tgt_yaml_file)
#     write_dbt_source_file(tables_used_by_dataset, tgt_yaml_file)




# test = 0 