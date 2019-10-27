from flask import Flask, request
import argparse
import os
import os.path as osp
import requests

global tablets
global locks

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
        url = com_url(tablet['hostname'], tablet['port'], '/api/tables/{}'.format(Table_name))
        requests.delete(url)
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


if __name__ == '__main__':
    global tablet1
    global tablet2
    global tablet3
    global locks
    parser = get_args_parser()
    args = parser.parse_args()
    tablet1 = {'host': args.tablet1_hostname, 'port': args.tablet1_port}
    tablet2 = {'host': args.tablet2_hostname, 'port': args.tablet2_port}
    tablet3 = {'host': args.tablet3_hostname, 'port': args.tablet3_port}
    locks = dict()
    app.run(args.master_hostname, args.master_port)
