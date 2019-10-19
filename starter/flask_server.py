from flask import Flask, request
import argparse
import os
import os.path as osp
import table_api
import json
import global_v as Global

global metadata
global memtable
global memindex

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'hello world'


@app.route('/api/tables', methods=['GET'])
def get_list_tables():
    global metadata
    res = {'tables': list(metadata.keys())}
    return res, 200


@app.route('/api/tables/<Table_name>', methods=['GET'])
def get_table_info(Table_name):
    global metadata
    if Table_name in metadata:
        table_info = metadata[Table_name]
        res = {key: table_info[key] for key in metadata[Table_name] if key != 'filenames'}
        return res, 200
    else:
        return "", 404


@app.route('/api/tables/<Table_name>', methods=['DELETE'])
def table_delete(Table_name):
    global metadata
    try:
        table_api.delete_table(Table_name, metadata)
    except NameError:
        return "", 404
    return "", 200


@app.route('/api/tables', methods=['POST'])
def post_create_table():
    table_schema = request.get_json(force=True, silent=True)
    if table_schema is None:
        return "", 400
    global metadata
    try:
        table_api.create_table(table_schema, metadata)
    except NameError:
        return "", 409
    return "", 200


@app.route('/api/table/<table_name>/cell', methods=['POST'])
def post_insert_cell(table_name):
    global metadata
    payload = request.get_json(force=True, silent=True)
    if table_name not in metadata:
        return '', 404
    column_family_key = payload['column_family']
    table_info = metadata[table_name]
    column_family_info = [column_family_info for column_family_info in
                          table_info['column_families'] if column_family_info['column_family_key'] == column_family_key]
    if not len(column_family_info):
        return '', 400
    assert len(column_family_info) == 1
    column_family_info = column_family_info[0]
    column_key = payload['column']
    if column_key not in column_family_info['columns']:
        return '', 400

    return '', 200


def get_args_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('tablet_hostname', type=str, help='tablet hostname address')
    parser.add_argument('tablet_port', type=int, help='tablet port number')
    parser.add_argument('master_hostname', type=str, help='master hostname address')
    parser.add_argument('master_port', type=int, help='master port number')
    parser.add_argument('wal', type=str, help='path to Write Ahead Log (WAL) file')
    parser.add_argument('sstable_folder', type=str, help='path to SSTable folder')
    return parser


def main():
    parser = get_args_parser()
    args = parser.parse_args()

    wal_path = args.wal
    sstable_folder = args.sstable_folder
    metadata_path = osp.join(sstable_folder, 'metadata.json')
    ssindex_path = osp.join(osp.split(wal_path)[0], 'ssindex.json')

    Global.set_wal_path(wal_path)
    Global.set_sstable_folder(sstable_folder)
    Global.set_metadata_path(metadata_path)

    if not osp.exists(wal_path):
        os.mknod(wal_path)

    if not osp.exists(sstable_folder):
        os.makedirs(sstable_folder)

    if not osp.exists(metadata_path):
        with open(metadata_path, 'w+') as fp:
            fp.write('{}')

    if not osp.exists(ssindex_path):
        with open(ssindex_path, 'w+') as fp:
            fp.write('{}')

    global metadata
    with open(metadata_path, 'r') as fp:
        metadata = json.load(fp)

    app.run(args.tablet_hostname, args.tablet_port)


if __name__ == '__main__':
    main()
