from collections import defaultdict
import functions
import json

def write_source_file(tgt_file: str, model_tables=defaultdict(list)):
    dbt_version_file = r"2"
    print("build source yml file: " + tgt_file)
    print(json.dumps(model_tables, indent = 4))
    functions.write_source_file_from_TableDef(model_tables, tgt_file,dbt_version_file)