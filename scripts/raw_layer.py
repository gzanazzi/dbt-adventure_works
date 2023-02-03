from collections import defaultdict
from dataclasses import dataclass
import json
import sys
import os
from traceback import print_tb
import yaml
import openpyxl
import functions



def write_raw_layer(wbtd, tables=defaultdict(), tables_bq=defaultdict() , test=0):
   
    # print(json.dumps(tables, indent = 4))
    # print("---------------")
    # print(json.dumps(tables_bq, indent = 4))
    # for ktab, vschema in tables.items():
    #     print( ktab + "   " + vschema)
        
    for ktab, vschema in tables.items():
        print("write_raw_layer" + ktab + "   " + vschema)
        query_stmt = "select \n"
        SourceColumns=[]
        SourceTableColumnsKey=[]
        cl=[]
        columns_stmt=""
        
        # #print("====================  "  +model_name)
        print("search table " + ktab + " in whorbook sheets")
        if ktab in wbtd.sheetnames:
            print(" TABLE FIND: build raw query for: " + vschema + " " + ktab)
            #print(rstab + " foglio trovato")
            functions.get_columns_from_Tables_definitions(wbtd[ktab],SourceColumns)
            columns_stmt=""
            for r in SourceColumns:
                #print(r.col_name)
                #print(r.p_key)
                if columns_stmt=="":
                    columns_stmt = r.col_name + "\n"
                else:
                    columns_stmt += ", " + r.col_name + "\n"
                if r.p_key is True:
                    SourceTableColumnsKey.append(r.col_name)
                    #print(list(SourceTableColumnsKey))
                if r.description:
                        desc=""+r.description
                else:
                    desc=""
                c = {"name" : r.col_name, "description":  desc}
                cl.append(c)

                # if test == 1:
                #      print(json.dumps(c,indent=4))
        
        query_stmt+=columns_stmt
        # query_stmt+=", {{sdgdbt.raw_audit_fields()}}\n"

        config={
            "alias": 'raw_' + ktab
        }

        if SourceTableColumnsKey:
            #print(list(SourceTableColumnsKey))
            meta={
                "primary_key": list(SourceTableColumnsKey)
            }
        else:
            meta={}

        # tests=["sdgdbt.primary_key"]

        # -------------- from statement query
        if ktab in tables_bq.keys():
            tab_bq = tables_bq[ktab]
        else:
            tab_bq = ktab
        query_stmt += f"from {{{{ source('{vschema.lower()}', 'seed_{tab_bq.lower()}') }}}} \n" 
        
        query_stmt += f"where 1 = 1 \n"
        # query_stmt += f"and {{{{slt_incremental_filter()}}}}"
        # query_stmt += f"{{{{sdgdbt.qualify_last_version(sorting_columns=['recordstamp'])}}}}"
       
        if test == 1:
            print(query_stmt)
        
        # -------------- write model sql file
        mf_path = r"models/raw/"
        mf_name = ktab.lower() + "__raw"
        mf_name_sql = mf_name+".sql"
        if os.path.exists(mf_path+mf_name_sql):
            os.remove(mf_path+mf_name_sql)
        mf = open(mf_path+mf_name_sql, mode='w', encoding=sys.getdefaultencoding())
        
        if test == 1:
            print(query_stmt)
        else:
            mf.write(query_stmt)


        #------------- write model yml file
        # if test ==1:
        #     print(cl[0])
        #     print(cl[-1])             


        #write_yaml_dump(base_model_yml(mf_name,config,meta,tests=tests),mf_path+mf_name+".yml")
        if test ==1:
            print("non scrivo yml")
        else:
            functions.write_yaml_dump(functions.base_model_yml_with_columns(mf_name,config,meta,cl),mf_path+mf_name+".yml") # functions.write_yaml_dump(functions.base_model_yml_with_columns(mf_name,config,meta,cl,tests),mf_path+mf_name+".yml")
            