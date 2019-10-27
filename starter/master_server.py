from flask import Flask, request
import argparse
import os
import os.path as osp
import requests
import master_api
import json

global metadata
global metadata_path
global tablet
global tablet_reverse

app = Flask(__name__)


def get_args_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('master_hostname', type=str, help='master hostname address')
    parser.add_argument('master_port', type=int, help='master port number')
    return parser


@app.route('/api/tables', methods=['POST'])
def post_create_table():
    global metadata
    global metadata_path
    table_schema = request.get_json(force=True, silent=True)
    if table_schema is None:
        return "", 400
    if table_schema["name"] in metadata:
        return "", 409
    tablet_name = "tablet" + str(len(metadata) % len(tablet) + 1)
    master_api.create_table(tablet[tablet_name]["host"], tablet[tablet_name]["port"], table_schema)
    metadata[table_schema["name"]] = {"name": table_schema["name"], "tablets": [{ "hostname": tablet[tablet_name]["host"], "port": str(tablet[tablet_name]["port"]), "row_from": "", "row_to": ""}]}
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f)
    return "", 400

@app.route('/api/sharding/<host>/<port>/<table_name>/<midle_row>', methods=['POST'])
def post_sharding(host, port, table_name, midle_row):
    global metadata
    global metadata_path
    global tablet
    global tablet_reverse
    index = request.get_json(force = True, silent = True)
    row_from = ''
    row_to = ''
    # tablet_name = tablet_reverse[]
    if index is None:
        return "", 400

    # for tablet in metadata[table_name]["tablets"]:
    #      if tablet["hostname"] + "_" + tablet["port"] == host + "_" + str(post):
    #          row_from = tablet["row_from"]
    #          row_to = tablet["row_to"]

@app.route('/api/tablet', methods=['POST'])
def create_tablet():
    global tablet
    global tablet_reverse
    host_port = request.get_json(force = True, silent = True)
    tablet_num = len(tablet)
    tablet["tablet" + str(tablet_num + 1)] = {"host" : host_port["host"], "port": host_port["port"]}
    tablet_reverse[host_port["host"] + "_" + str(host_port["port"])] = "tablet" + str(tablet_num + 1)

if __name__ == '__main__':
    global tablet
    global metadata
    global metadata_path
    global tablet_reverse
    parser = get_args_parser()
    args = parser.parse_args()
    
    metadata_path = osp.join(osp.split(__file__)[0], "metadata.json")
    if not osp.exists(metadata_path):
        with open(metadata_path, 'w+') as fp:
            fp.write('{}')

    with open(metadata_path, 'r') as fp:
        metadata = json.load(fp)

    app.run(args.master_hostname, args.master_port)
