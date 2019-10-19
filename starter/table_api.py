import json
from typing import List
import os
import os.path as osp
import global_v as Global


def create_table(table_schema: str, mem_metadata):
    metadata_path = Global.get_metadata_path()
    sstable_folder = Global.get_sstable_folder()

    table_name = table_schema['name']

    if table_name in mem_metadata.keys():
        raise NameError('Table Already Exists')

    table_filename = '{}_{}.json'.format(table_name, 1)
    os.mknod(osp.join(sstable_folder, table_filename))

    table_schema['filenames'] = [table_filename]
    mem_metadata[table_name] = table_schema

    with open(metadata_path, 'w') as fp:
        json.dump(mem_metadata, fp)


def delete_table(Table_name, mem_metadata):
    if Table_name in mem_metadata:
        Table_list = mem_metadata[Table_name]['filenames']
        for Table in Table_list:
            Table_path = osp.join(Global.get_sstable_folder(), Table)
            os.remove(Table_path)
        mem_metadata.pop(Table_name)
        with open(Global.get_metadata_path(), 'w') as f:
            json.dump(mem_metadata, f)
    else:
        raise NameError("Table does not exist!")
