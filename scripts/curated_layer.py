from collections import defaultdict
from dataclasses import dataclass
import json
import sys
import os
import yaml
import openpyxl
import functions


def write_curated_layer(wbtd, tables=defaultdict(),test=0):
    for ktab, vschema in tables.items():
        #print(ktab + "   " + vschema)
        query_stmt = "select \n"
            # #curr_table = rschema + "." + rstab
            # #get_rows_by_source(rschema, rstab, column_and_tables_sheet)
            # #print(model_name.upper())
        SourceColumns=[]
        SourceTableColumnsKey=[]
        cl=[]
        columns_stmt=""
        
        # #print("====================  "  +model_name)
        if ktab in wbtd.sheetnames:
            print("build curated query for: " + vschema + " " + ktab)
            functions.get_columns_from_Tables_definitions(wbtd[ktab],SourceColumns)
            columns_stmt=""
            for r in SourceColumns:
                if r.alias_name:
                    #print(r.col_name)
                    #print(r.p_key)
                    if r.col_name != r.alias_name:
                        c_name = r.col_name + " as " + r.alias_name.lower()
                    else:
                        c_name = r.col_name
                    if columns_stmt=="":
                        columns_stmt = c_name + "\n"
                    else:
                        columns_stmt += ", " + c_name + "\n"
                    # ------ for the moment no keys are managed
                    # if r.p_key is True:
                    #     if r.alias_name:
                    #         c_name = r.alias_name.lower()
                    #     SourceTableColumnsKey.append(c_name)
                    if r.description:
                        desc=""+r.description
                    else:
                        desc=""
                    c = {"name" : r.alias_name.lower(), "description":  desc}
                    cl.append(c)

                    # if test == 1:
                    #      print(json.dumps(c,indent=4))
        
        query_stmt+=columns_stmt
        # query_stmt+=", {{sdgdbt.raw_audit_fields()}}\n"

        config={
            "alias": 'curated_' + ktab
        }

        # if SourceTableColumnsKey:
        #     print(list(SourceTableColumnsKey))
        #     meta={
        #         "primary_key": list(SourceTableColumnsKey)
        #     }
        # else:
        meta={}

        tests=[]
        
        # -------------- from statement query
        query_stmt += f"from {{{{ ref('{ktab.lower()}__raw') }}}}"
        
        if test == 1:
            print(query_stmt)
        
        # -------------- write model sql file
        mf_path = r"models/curated/"
        mf_name = ktab.lower() + "__curated"
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
            functions.write_yaml_dump(functions.base_model_yml_with_columns(mf_name,config,meta,cl,tests),mf_path+mf_name+".yml")





# ----------------  OLD VERSION - CURATED LAYER -----------------------------
# write_curated=False
# if write_curated:
#     print("build curated layer")
#     for rschema in tables_used_by_dataset:
#     # print(rschema)
#         for rstab in tables_used_by_dataset[rschema]:
#             #start for
#             model_name = source_table_to_base_name(rstab)
            
#             #curr_table = rschema + "." + rstab
#             #print(curr_table + " model = " +model_name)

#             config={
#                 "alias":model_name
#             }


#             sel_cols = get_columns_by_source(rschema, rstab, column_and_tables_sheet)

#             #for c in sel_cols["/POSDW/TLOGF"]:
#             #     print(c)

#             first = 1  # manage first column
#             cols_stmt=""
#             # ---------------- loop to search table columns
#             for c in sel_cols[rstab]:
#                 if first == 1:
#                     cols_stmt = "    " + c + "\n"
#                     first = 0
#                 else:
#                     cols_stmt += "    , " + c + "\n"
#                 # if rstab == "/POSDW/TLOGF":
#                 #     print(c)
#                     #print(cols_stmt)
                
#             cols_stmt += ", {{sdgdbt.raw_audit_fields(name_only=True)}}\n"
            
#             # -------------- from statement query
#             #query_stmt+="from {{ source('src_" + rschema + "', '" + rstab + "') }}"
#             query_stmt = "select \n" + cols_stmt + f"from {{{{ ref('{model_name}__raw') }}}}"

            
#             if test == 1:
#                 print(query_stmt)
            
#             # -------------- write query into sql file
#             mf_path = r"models/curated/"
#             mf_name = model_name + "__curated"
#             mf_name_sql = mf_name + ".sql"
#             if os.path.exists(mf_path+mf_name_sql):
#                 os.remove(mf_path+mf_name_sql)
#             mf = open(mf_path+mf_name_sql, mode='w', encoding=sys.getdefaultencoding())
#             mf.write(query_stmt)

#             write_yaml_dump(base_model_yml(mf_name,config,{}),mf_path+mf_name+".yml")
#             #end for
        
