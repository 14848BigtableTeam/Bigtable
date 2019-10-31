"""

master_server.py

Author:
    Can Xia (Andrew ID: canx)
    Chi Zhang (Andrew ID: czhang5)

Implement several manipulations in master server of BigTable, including
open/close tables (related to locks on tables), list all existing tables,
get table's information (including information of tablets the table
distributed to, and the range of row keys), and create/delete tables.
To achieve the full functionality, we also define several helper apis
and a sub-threading. This module basally use the Flask as the http
server to listen on difference http request. To run this master,
master hostname and port number need to be provided as the command
line arguments. This module will create a file named 'metadata.json'
in the current directory, which includes some information about table
information.

"""

import copy
import threading

from flask import Flask, request
import argparse
import os.path as osp
import requests
import json
import time

import master_api

global locks
global metadata
global metadata_path
global tablets
global tablets_reverse
global wal_list
global ssindex_list
global metadata_list

app = Flask(__name__)


@app.route('/api/lock/<Table_name>', methods=['POST'])
def open_table(Table_name):
    """
    Open a table (add read lock on the opened table)
    :param Table_name: the name of opened table
    :return: empty response, status code
    """
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
        locks[Table_name].add(client_id)
        return '', 200
    # Table already opened
    else:
        return '', 400


@app.route('/api/lock/<Table_name>', methods=['DELETE'])
def close_table(Table_name):
    """
    Close a table (release a read lock on the closed table)
    :param Table_name: the name of opened table
    :return: empty response, status code
    """
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
    """
    List all existing tables
    :return: names of all existing tables
    """
    global metadata
    return {'tables': list(metadata.keys())}, 200


@app.route('/api/tables/<Table_name>/', methods=['GET'])
def get_table_info(Table_name):
    """
    Given a table name, get related information of this table
    :param Table_name: the name of queried table
    :return: related metadata of queried table, including address
    of tablets the table distributed to, and the range of row keys.
    """
    global metadata
    if Table_name not in metadata:
        return '', 404
    return metadata[Table_name], 200


@app.route('/api/tables/<Table_name>', methods=['DELETE'])
def destroy_table_info(Table_name):
    """
    Destroy a existing table
    :param Table_name: the name of destroyed table
    :return: blank response and status code
    """
    global metadata
    global metadata_path
    global locks
    # Table not exist
    if Table_name not in metadata:
        return '', 404
    # Table in use
    if len(locks.get(Table_name, set())):
        return '', 409
    # Destroy tables in all tablets
    for tablet in metadata[Table_name]['tablets']:
        # Send delete requests to all tablets which the destroyed table distributed to
        delete_url = master_api.com_url(tablet['hostname'], tablet['port'], '/api/tables/{}'.format(Table_name))
        requests.delete(delete_url)
    # Delete information in metadata
    metadata.pop(Table_name)
    with open(metadata_path, 'w') as fp:
        json.dump(metadata, fp)
    return '', 200


def get_args_parser():
    """
    Construct the command line argument parser
    :return: command line argument parser object
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('master_hostname', type=str, help='master hostname address')
    parser.add_argument('master_port', type=int, help='master port number')
    return parser


@app.route('/api/tables', methods=['POST'])
def post_create_table():
    global metadata
    global metadata_path
    global tablets
    table_schema = request.get_json(force=True, silent=True)
    if table_schema is None:
        return "", 400
    if table_schema["name"] in metadata:
        return "", 409
    tablet_name = "tablet" + str(len(metadata) % len(tablets) + 1)
    master_api.create_table(tablets[tablet_name]["host"], tablets[tablet_name]["port"], table_schema)
    metadata[table_schema["name"]] = {"name": table_schema["name"], "tablets": [
        {"hostname": tablets[tablet_name]["host"], "port": str(tablets[tablet_name]["port"]), "row_from": "",
         "row_to": ""}]}
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f)
    return "", 200


@app.route('/api/sharding/<host>/<port>/<table_name>/<midle_row>', methods=['POST'])
def post_sharding(host, port, table_name, midle_row):
    global metadata
    global metadata_path
    global tablets
    global tablets_reverse
    index = request.get_json(force=True, silent=True)
    row_to = ''
    tablet_name = tablets_reverse[host + "_" + str(port)]
    tablet_number = int(tablet_name[-1:])
    tablet_list = []
    if index is None:
        return "", 400

    for one_tablet in metadata[table_name]["tablets"]:
        if one_tablet["hostname"] + "_" + one_tablet["port"] == host + "_" + str(port):
            row_to = one_tablet["row_to"]
            one_tablet["row_to"] = midle_row
        tablet_list.append(tablets_reverse[one_tablet["hostname"] + "_" + one_tablet["port"]])

    for i in range(len(tablets)):
        sharding_tablet_number = (tablet_number + i) % len(tablets) + 1
        sharding_tablet = "tablet" + str(sharding_tablet_number)
        if sharding_tablet not in tablet_list:
            metadata[table_name]["tablets"].append(
                {"hostname": tablets[sharding_tablet]["host"], "port": str(tablets[sharding_tablet]["port"]),
                 "row_from": midle_row,
                 "row_to": row_to})
            url = master_api.com_url(tablets[sharding_tablet]["host"],
                                     tablets[sharding_tablet]["port"], '/api/sharding/' + table_name)
            requests.post(url, json=index)
            break
    with open(metadata_path, 'w') as fp:
        json.dump(metadata, fp)
    return "", 200


@app.route('/api/tablet', methods=['POST'])
def create_tablet():
    global tablets
    global tablets_reverse
    global metadata_list
    global ssindex_list
    global wal_list
    host_port = request.get_json(force=True, silent=True)
    if host_port['host'] + '_' + str(host_port['port']) not in tablets_reverse:
        tablet_num = len(tablets)
        tablets["tablet" + str(tablet_num + 1)] = {"host": host_port["host"], "port": host_port["port"]}
        tablets_reverse[host_port["host"] + "_" + str(host_port["port"])] = "tablet" + str(tablet_num + 1)
        wal_list["tablet" + str(tablet_num + 1)] = host_port["wal"]
        ssindex_list["tablet" + str(tablet_num + 1)] = host_port["ssindex"]
        metadata_list["tablet" + str(tablet_num + 1)] = host_port["metadata"]
    return '', 200


def check_connected():
    """
    The sub-threading of this module, it will check if a connected tablet is still connected.
    If it's no longer connected, master can transfer its data to another tablet.
    :return: None
    """
    global tablets
    global metadata
    global metadata_path
    recoveried = []
    while (True):
        tablets_copy = copy.copy(tablets)
        for tablet_name, host_port in tablets_copy.items():
            connect_url = 'http://{}:{}/api/connect'.format(host_port['host'], host_port['port']);
            try:
                # Try to send a request to check the connection.
                connect_resp = requests.get(connect_url)
            except requests.exceptions.ConnectionError as e:
                # Connection shutdown
                if tablet_name not in recoveried:
                    recoveried.append(tablet_name)
                    recovery_candidates = [one_tablet_name for one_tablet_name in tablets if
                                           one_tablet_name != tablet_name]
                    if len(recovery_candidates):
                        recovery_tablet_name = recovery_candidates[0]
                        # Modify metadata
                        for table_name in metadata:
                            metadata[table_name]['tablets'] = [one_tablet for one_tablet in
                                                               metadata[table_name]['tablets']
                                                               if
                                                               one_tablet['hostname'] != host_port['host'] and
                                                               one_tablet[
                                                                   'port'] != host_port['port']]
                            if len(metadata[table_name]['tablets']) == 0:
                                metadata[table_name]['tablets'] = [{'hostname': tablets[recovery_tablet_name]['host'],
                                                                    'port': tablets[recovery_tablet_name]['port'],
                                                                    'row_from': '', 'row_to': ''}]
                            with open(metadata_path, 'w') as fp:
                                json.dump(metadata, fp)
                        # Send recovery request
                        recovery_tablet_host_port = tablets[recovery_tablet_name]
                        recovery_url = 'http://{}:{}/api/recovery'.format(recovery_tablet_host_port['host'],
                                                                          recovery_tablet_host_port['port'])
                        recovery_payload = {
                            'ssindex': ssindex_list[tablet_name],
                            'wal': wal_list[tablet_name],
                            'metadata': metadata_list[tablet_name],
                        }
                        requests.post(recovery_url, json=recovery_payload)
        # Run the check every 10 seconds
        time.sleep(10)


if __name__ == '__main__':
    global locks
    global tablets
    global metadata
    global metadata_path
    global tablets_reverse
    parser = get_args_parser()
    args = parser.parse_args()

    # Initialized global variables
    locks = dict()
    tablets = {}
    tablets_reverse = {}
    ssindex_list = {}
    wal_list = {}
    metadata_list = {}

    # Create metadata file
    metadata_path = osp.join(osp.split(__file__)[0], "metadata.json")
    if not osp.exists(metadata_path):
        with open(metadata_path, 'w+') as fp:
            fp.write('{}')

    with open(metadata_path, 'r') as fp:
        metadata = json.load(fp)

    # Start connection check thread
    check_conn_thread = threading.Thread(target=check_connected, name='ConnectCheck')
    check_conn_thread.start()

    # Run server
    app.run(args.master_hostname, args.master_port)
