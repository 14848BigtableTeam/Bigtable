import json
from typing import List
import os
import os.path as osp
import global_v as Global


def get_table_info(Table_name):
    
    with open(Global.get_metadata_path(), 'r') as f:
        metadata = json.load(f)
    
    if Table_name in metadata:
        metadata[Table_name].pop('filenames')
        print(metadata[Table_name])
        return metadata[Table_name]
    else:
        raise NameError("Table does not exist!")

def list_tables() -> List[str]:
    with open(Global.get_metadata_path(), 'r') as fp:
        metadata: dict = json.load(fp)
    return list(metadata.keys())


def create_table(table_schema: str):
    METADATA_PATH = Global.get_metadata_path()
    SSTABLE_FOLDER = Global.get_sstable_folder()

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

def delete_table(Table_name):

    with open(Global.get_metadata_path(), 'r') as f:
        metadata = json.load(f)

    if Table_name in metadata:    
        Table_list = metadata[Table_name]['filenames']
        print(Table_list)
        for Table in Table_list:
            Table_path = osp.join(Global.get_sstable_folder(), Table)
            os.remove(Table_path)
        metadata.pop(Table_name)
        with open(Global.get_metadata_path(), 'w') as f:
            json.dump(metadata, f)
    else:
        raise NameError("Table does not exist!")
