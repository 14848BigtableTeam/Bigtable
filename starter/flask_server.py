from flask import Flask, request
import argparse
import os
import os.path as osp
import api
import global_v as Global

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'hello world'


@app.route('/api/tables', methods=['GET'])
def get_list_tables():
    res = {'tables': api.list_tables()}
    return res, 200


@app.route('/api/tables/<Table_name>', methods=['GET'])
def table_info(Table_name):
    try:
        res = api.get_table_info(Table_name)
    except NameError:
        return "", 404
    return res, 200


@app.route('/api/tables/<Table_name>', methods=['DELETE'])
def table_delete(Table_name):
    try:
        api.delete_table(Table_name)
    except NameError:
        return "", 404
    return "", 200


@app.route('/api/tables', methods=['POST'])
def post_create_table():
    table_schema = request.get_json(force=True, silent=True)
    if table_schema is None:
        return "", 400
    try:
        api.create_table(table_schema)
    except NameError:
        return "", 409
    return "", 200


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

    app.run(args.tablet_hostname, args.tablet_port)


if __name__ == '__main__':
    main()
