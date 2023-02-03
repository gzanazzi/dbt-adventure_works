from collections import defaultdict
from dataclasses import dataclass
#import json
import sys
import os
import yaml
import openpyxl

# ------------------------------------- FUNCTION: write yml file from python dictionary with yaml.dump

#------ mappings ---------

def bq_schema_from_source_schema(src_schema):
    ds_map = {
        "NAIS_FIN_CAR": "car_data", "NAIS_BI": "bi_data", "NAIS_FIN_CRM": "fms_data"
    }
    return ds_map.get(src_schema)

def bq_table_from_source_schema(src_table):
    ds_map = {
        "TLOGF": "posdw_tlogf", "TLOGF_EXT": "posdw_tlogf_ext", "PLOG1S": "posdw_plog1s"
    }
    return ds_map.get(src_table.upper())

def get_bq_table_name_from_xls(src_table,map_dict=defaultdict(list)):
    if src_table in map_dict.keys():
        tab_bq = map_dict[src_table]
    else:
        tab_bq = src_table
    return tab_bq

#----- modeling -----------

def source_table_to_model_name(src_table: str):
    return src_table.replace("/", "_").strip("_").lower()


def source_table_to_base_name(src_table: str, apply_map: bool = True):
    #print(src_table)
    t_name = src_table.split("/")[-1].lower()
    # if test ==1:
    #     print(t_name)
    #     print(bq_table_from_source_schema(t_name))
    if apply_map and bq_table_from_source_schema(t_name) is not None:
        t_name = bq_table_from_source_schema(t_name)
    return t_name

def write_yaml_dump(source_data, tgt_file):
    with open(tgt_file, mode='w', encoding=sys.getdefaultencoding()) as file:
        yaml.dump(source_data, file, default_flow_style=False,sort_keys=False)
        #yaml.safe_load(source_row, file)


    

def base_model_yml(name, config, meta, tests=[]):
    return {
        "version": 2,
        "models": [
            {
                "name": name,
                "config": config,
                "meta": meta,
                "tests": tests
            }

        ],

    }



def base_model_yml_with_columns(name, config, meta, cols=[], tests=[]):
    #print(list(cols))
    return {
        "version": 2,
        "models": [
            {
                "name": name,
                "config": config,
                "meta": meta,
                "tests": tests,
                "columns": cols
            }
        ]
    }



# ------------------------------------- FUNCTION: write yml file from python dictionary with txt string

def write_dbt_source_file(tables_dict, tgt_file,dbt_version_file):
    yaml_txt = "version: " + " " + dbt_version_file + "\n"
    yaml_txt += "\n"
    yaml_txt += "sources: " + "\n"
   
    #print(json.dumps(tables_dict, indent=4))
    
    for rschema in tables_dict:
        if rschema is None:
            continue
        print(rschema)
        bq_schema = bq_schema_from_source_schema(rschema)
        yaml_txt += "\n"
        yaml_txt += "- name: " + bq_schema + "\n"
        yaml_txt += "  schema: " + bq_schema + "\n"
        yaml_txt += "  tables: " + "\n"
        for rtable in tables_dict[rschema]:
            #print(rschema + "." +rtable)
            bq_table_name = source_table_to_base_name(rtable,True).upper()
            #print(rschema, '.',rtable)
            yaml_txt += "      - identifier: " + bq_table_name + "\n"
            yaml_txt += "        name: " + bq_table_name.lower() + "\n"

    # if test == 1:
    #     print(yaml_txt)

    if os.path.exists(tgt_file):
        os.remove(tgt_file)
    f = open(tgt_file, mode='w', encoding=sys.getdefaultencoding())
    f.write(yaml_txt)

def write_source_file_from_TableDef(tables_dict, tgt_file,dbt_version_file):
    yaml_txt = "version: " + " " + dbt_version_file + "\n"
    yaml_txt += "\n"
    yaml_txt += "sources: " + "\n"
   
    #print(json.dumps(tables_dict, indent=4))
    
    for rschema in tables_dict:
        if rschema is None:
            continue
        yaml_txt += "\n"
        yaml_txt += "- name: " + rschema + "\n"
        yaml_txt += "  schema: " + rschema + "\n"
        yaml_txt += "  tables: " + "\n"
        for rtable in tables_dict[rschema]:
            #print(rschema, '.',rtable)
            yaml_txt += "      - identifier: seed_" + rtable + "\n" # yaml_txt += "      - identifier: " + rtable + "\n"
            yaml_txt += "        name: seed_" + rtable.lower() + "\n" # yaml_txt += "        name: " + rtable.lower() + "\n"

    # if test == 1:
    #     print(yaml_txt)

    if os.path.exists(tgt_file):
        os.remove(tgt_file)
    f = open(tgt_file, mode='w', encoding=sys.getdefaultencoding())
    f.write(yaml_txt)



# ------------------------------------- FUNCTION: get rows (columns) of one particular table "tab"
# prerequisite: excel file formatted with fixed columns

selected_columns = defaultdict(list)


def get_columns_by_source(pschema: str, ptab: str, ws):
    
    for _row in ws.values:
        if _row[7] == pschema and _row[8] == ptab and not _row[6] == "Y":
            selected_columns[ptab]
            c_name = _row[3]
            c_alias = _row[4]
            if isinstance(c_alias, str) and c_name != c_alias:
                c_name += " as " + c_alias
            if c_name not in selected_columns[ptab]:              
                selected_columns[ptab].append(c_name)
                #name=_row[2], data_type=_row[7],alias=_row[3]
    return selected_columns


@ dataclass
class QueryElement:
    T_name: str
    T_alias: str
    is_master: bool
    where_clause: str
    join_type: str
    join_clause: str
    columns: str
    group_by: str

    # def __init__(self, p_name, p_alias,p_is_master,p_where_clause,p_join_clause):
    #     self.T_name =  p_name
    #     self.T_alias  = p_alias
    #     self.is_master = p_is_master
    #     self.where_clause = p_where_clause
    #     self.join_type
    #     self.join_clause = p_join_clause

    @ classmethod
    def query_detail(cls, row):
        return cls(T_name=row[2] or row[7], T_alias=row[3] or row[8], is_master=row[2] and len(row[2]) > 0, where_clause=row[9], join_clause=row[10], join_type=row[5], columns=row[0], group_by=row[12])

# ------------------------------------- FUNCTION: build a query from xls sheet formatted(*)


def query_builder_from_xls(sheet):
    rows = list(sheet.values)

    sql_stmt = "select \n"
    columns_stmt = ""
    from_stmt = ""
    join_stmt = ""
    where_stmt = "\nwhere "
    group_by_stmt = ""
    for row in rows[1:]:
        query_row = QueryElement(T_name=row[2] or row[7],
                                 T_alias=row[3] or row[8],
                                 is_master=row[2],
                                 where_clause=row[4] or row[9],
                                 join_type=row[5],
                                 join_clause=row[10],
                                 columns=row[0],
                                 group_by=row[12])
        #print(query_row.T_name)
       # table_model_name_map = source_table_to_base_name(query_row.T_name,True)
        if isinstance(query_row.columns, str):
            columns_stmt += query_row.columns
            if query_row.columns.count("\n") == 0:
                columns_stmt += "\n"

        if query_row.is_master is not None:
            from_stmt = "\nfrom " + query_row.T_name + " as " + query_row.T_alias + "\n"

        else:
            if isinstance(query_row.join_type, str) and query_row.join_type[:2] == "LO":
                join_stmt += "\nleft join "
            elif isinstance(query_row.join_type, str) and query_row.join_type[:2] == "IJ":
                join_stmt += '\ninner join '
            if isinstance(query_row.T_name, str):
                join_stmt += query_row.T_name
            if isinstance(query_row.T_alias, str):
                join_stmt += " as " + query_row.T_alias + " on\n"
            if isinstance(query_row.join_clause, str):
                join_stmt += query_row.join_clause + "\n"
                if not query_row.is_master and isinstance(query_row.where_clause, str):
                    join_stmt += "AND " + query_row.where_clause + "\n"
        if isinstance(query_row.where_clause, str) and query_row.is_master:
            where_stmt += query_row.where_clause

        if isinstance(query_row.group_by, str):
            group_by_stmt += query_row.group_by

    sql_stmt += columns_stmt + " " + from_stmt + \
        join_stmt + where_stmt + group_by_stmt

    return sql_stmt

# ------------------------------------- FUNCTION: get column details fron formatted xls)
# print(list(QueryElement.query_detail(tab_ws,row)))
@ dataclass
class table_columns:
    col_name: str
    alias_name: str
    description: str
    p_key: bool

# @ dataclass
# class table_columns:
#     col_name: str
#     alias_name: str
#     data_type: str
#     description: str
#     p_key: bool

def get_columns_from_Tables_definitions(s:openpyxl.worksheet.worksheet.Worksheet,cols = []):
    # based structure of excel file https://docs.google.com/spreadsheets/d/1lfwOMs0VDgWcSUUVxvYOiVPHK3A40z-P/edit?usp=sharing&ouid=116467095844044196451&rtpof=true&sd=true
    if s.title:
        rows = list(s.values)
        for row in rows[1:]:
            pk=False
            if row[2]: # if row[5]:
                pk=True
            cols.append(table_columns(row[0],row[1],row[3],pk)) # cols.append(table_columns(row[0],row[1],row[2],row[8],pk))
    
    # if test == 1:
    #     print(list(cols)) 

