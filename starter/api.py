import json
from typing import List
import os
import os.path as osp

global WAL_PATH
global SSTABLE_FOLDER
global METADATA_PATH

def get_table_info(str Table_name):
    global METADATA_PATH
    
    with open(METADATA_PATH, 'r') as f:
        metadata = json.load(f)
    
    if Table_name in metadata:
        return metadata[Table_name]
    else:
        raise NameError("Table does not exist!")
        return 

def list_tables() -> List[str]:
    with open(METADATA_PATH, 'r') as fp:
        metadata: dict = json.load(fp)
    return list(metadata.keys())


def create_table(table_schema_str: str):
    table_schema: dict = json.loads(table_schema_str)
    table_name = table_schema['name']

    with open(METADATA_PATH, 'r') as fp:
        metadata: dict = json.load(fp)

    if table_name in metadata.keys():
        raise NameError('Table Already Exists')

    table_filename = '{}_{}.json'.format(table_name, 1)
    os.mknod(osp.join(SSTABLE_FOLDER, table_filename))

    table_schema['filenames'] = [table_filename]
    metadata[table_name] = table_schema

    with open(METADATA_PATH, 'w') as fp:
        json.dump(metadata, fp)

def delete_table():
    global METADATA_PATH

    with open(METADATA_PATH, 'r') as f:
        metadata = json.load(f)

    if Table_name in metadata:
        Table_list = metadata[Table_name][filenames]
        for Table in Table_list:
            Table_path = os.join(SSTABLE_FOLDER, Table)
            os.remove(Table_path)
        modi_meta = metadata.pop(Table_name)
        with open(METADATA_PATH, 'w') as f:
            json.dump(modi_meta, f)
    else
        raise NameError("Table does not exist!")
    return
