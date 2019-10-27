from flask import Flask, request
import argparse
import os
import os.path as osp
import requests

global locks
import master_api
import json

global metadata
global metadata_path
global tablet

app = Flask(__name__)


@app.route('/api/lock/<Table_name>', methods=['POST'])
def open_table(Table_name):
    global metadata
    payload = request.get_json(force=True, silent=True)
    client_id = payload['client_id']
    # Table not exist
    if Table_name not in metadata:
        return '', 404
    # Table didn't open
    if client_id not in locks.get(Table_name, set()):
        if Table_name not in locks:
            locks[Table_name] = set()
        locks[Table_name].add(client_id);
        return '', 200
    # Table already opened
    else:
        return '', 400


@app.route('/api/lock/<Table_name>', methods=['DELETE'])
def close_table(Table_name):
    global metadata
    payload = request.get_json(force=True, silent=True)
    client_id = payload['client_id']
    # Table not exist
    if Table_name not in metadata:
        return '', 404
    # Table opened
    if client_id in locks.get(Table_name, set()):
        locks[Table_name].remove(client_id)
        return '', 200
    # Table didn't opened
    else:
        return '', 400


@app.route('/api/tables', methods=['GET'])
def get_list_table():
    global metadata
    return {'tables': list(metadata.keys())}, 200


@app.route('/api/tables/<Table_name>', methods=['GET'])
def get_table_info(Table_name):
    global metadata
    if Table_name not in metadata:
        return '', 404
    return metadata[Table_name], 200


@app.route('/api/table/<Table_name>', methods=['DELETE'])
def destroy_table_info(Table_name):
    global metadata
    global locks
    # Table not exist
    if Table_name not in metadata:
        return '', 404
    # Table in use
    if len(locks.get(Table_name, set())):
        return '', 409
    # Destroy tables in all tablets
    for tablet in metadata[Table_name]:
        delete_url = master_api.com_url(tablet['hostname'], tablet['port'], '/api/tables/{}'.format(Table_name))
        requests.delete(delete_url)
    metadata.pop(Table_name)
    return '', 200


def get_args_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('master_hostname', type=str, help='master hostname address')
    parser.add_argument('master_port', type=int, help='master port number')
    parser.add_argument('tablet1_hostname', type=str, help='tablet1 hostname address')
    parser.add_argument('tablet1_port', type=int, help='tablet1 port number')
    parser.add_argument('tablet2_hostname', type=str, help='tablet2 hostname address')
    parser.add_argument('tablet2_port', type=int, help='tablet2 port number')
    parser.add_argument('tablet3_hostname', type=str, help='tablet3 hostname address')
    parser.add_argument('tablet3_port', type=int, help='tablet3 port number')
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
    tablet_name = "tablet" + str(len(metadata) % 3 + 1)
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
    index = request.get_json(force = True, silent = True)
    if index is None:
        return "", 400
    tablet_list = []
    
    # for tablet_name in tablet:
        
    # for tablet in metadata[table_name]["tablets"]:
    #     if tablet 

if __name__ == '__main__':
    global locks
    global tablet
    global metadata
    global metadata_path
    parser = get_args_parser()
    args = parser.parse_args()
    tablet1 = {'host': args.tablet1_hostname, 'port': args.tablet1_port}
    tablet2 = {'host': args.tablet2_hostname, 'port': args.tablet2_port}
    tablet3 = {'host': args.tablet3_hostname, 'port': args.tablet3_port}

    locks = dict()

    tablet = {'tablet1': tablet1, "tablet2": tablet2, "tablet3": tablet3}
    metadata_path = osp.join(osp.split(__file__)[0], "metadata.json")
    if not osp.exists(metadata_path):
        with open(metadata_path, 'w+') as fp:
            fp.write('{}')

    with open(metadata_path, 'r') as fp:
        metadata = json.load(fp)

    app.run(args.master_hostname, args.master_port)
