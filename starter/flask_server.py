from flask import Flask, jsonify, request
import argparse
import os
import os.path as osp
import api
app = Flask(__name__)
import global_v as Global



@app.route('/')
def hello_world():
    return 'hello world'


@app.route('/api/tables', methods=['GET'])
def get_list_tables():
    res = {'tables': api.list_tables()}
    return res, 200

@app.route('./api/tables/<Table_name>', methods=['Get'])
def table_info(Table_name):
    data = request.get_json(force = True)
    



def get_args_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('tablet_hostname', type=str, help='tablet hostname address')
    parser.add_argument('tablet_port', type=int, help='tablet port number')
    parser.add_argument('master_hostname', type=str, help='master hostname address')
    parser.add_argument('master_port', type=int, help='master port number')
    parser.add_argument('WAL', type=str, help='path to Write Ahead Log (WAL) file')
    parser.add_argument('sstable_folder', type=str, help='path to SSTable folder')
    return parser


def main():
    parser = get_args_parser()
    args = parser.parse_args()

    WAL = args.WAL
    SSTABLE_FOLDER = args.sstable_folder
    METADATA_PATH = osp.join(SSTABLE_FOLDER, 'metadata.json')
    Global.set_wal_path(WAL)
    Global.set_sstable_folder(SSTABLE_FOLDER)
    Global.set_metadata_path(METADATA_PATH)

    if not osp.exists(WAL):
        os.mknod(WAL)

    if not osp.exists(SSTABLE_FOLDER):
        os.makedirs(SSTABLE_FOLDER)

    if not osp.exists(METADATA_PATH):
        os.mknod(METADATA_PATH)

    app.run(args.tablet_hostname, args.tablet_port)


if __name__ == '__main__':
    main()