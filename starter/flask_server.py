from flask import Flask
import argparse
import os
import os.path as osp
app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'hello world'


@app.route('/api/tables', methods=['GET'])
def list_table():
    pass




def get_args_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('tablet_hostname', type=str, help='tablet hostname address')
    parser.add_argument('tablet_port', type=int, help='tablet port number')
    parser.add_argument('master_hostname', type=str, help='master hostname address')
    parser.add_argument('master_port', type=int, help='master port number')
    parser.add_argument('WAL', type=str, help='path to Write Ahead Log (WAL) file')
    parser.add_argument('sstable_folder', type=str, help='path to SSTable folder')
    return parser


if __name__ == '__main__':
    parser = get_args_parser()
    args = parser.parse_args()

    global WAL_PATH
    WAL_PATH = args.WAL
    global SSTABLE_FOLDER
    SSTABLE_FOLDER = args.sstable_folder
    global METADATA_PATH
    METADATA_PATH = osp.join(SSTABLE_FOLDER, 'metadata.json')

    if not osp.exists(WAL_PATH):
        os.mknod(WAL_PATH)

    if not osp.exists(SSTABLE_FOLDER):
        os.makedirs(SSTABLE_FOLDER)

    if not osp.exists(METADATA_PATH):
        os.mknod(METADATA_PATH)

    app.run(args.tablet_hostname, args.tablet_port)
