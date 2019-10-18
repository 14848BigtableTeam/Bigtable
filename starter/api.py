import json
from typing import List
import os
import os.path as osp

global WAL_PATH
global SSTABLE_FOLDER
global METADATA_PATH


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
    pass
