import requests

def com_url(hostname, port, path):
    portstr = str(port)
    url = f"http://{hostname}:{portstr}{path}"
    return url


def create_table(host, port, table_schema):
    url = com_url(host, port, "/api/tables")
    requests.post(url, json=table_schema)