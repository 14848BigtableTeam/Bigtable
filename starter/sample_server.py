from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import argparse
import os
import os.path as osp

global WAL_PATH
global SSTABLE_FOLDER
global METADATA_PATH

class MyHandler(BaseHTTPRequestHandler):
    def _set_response(self, code):
        self.send_response(code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_GET(self):
        # example: this is how you get path and command
        print(self.path)
        print(self.command)

        # example: returning an object as JSON
        data = {
            "row": "sample_a",
            "data": [
                {
                    "value": "data_a",
                    "time": "1234"
                }
            ]
        }
        data_json = json.dumps(data)

        self._set_response(200)
        self.wfile.write(data_json.encode("utf8"))

    def do_POST(self):
        # example: reading content from HTTP request
        data = None
        content_length = self.headers['content-length']
    
        if content_length != None:
            content_length = int(content_length)
            data = self.rfile.read(content_length)

            # print the content, just for you to see it =)
            print(data)

        self._set_response(200)

    def do_DELETE(self):
        # example: send just a 200
        self._set_response(200)


def get_args_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('tablet_hostname', type=str, help='tablet hostname address')
    parser.add_argument('tablet_port', type=int, help='tablet port number')
    parser.add_argument('master_hostname', type=str, help='master hostname address')
    parser.add_argument('master_port', type=int, help='master port number')
    parser.add_argument('WAL', type=str, help='path to Write Ahead Log (WAL) file')
    parser.add_argument('sstable_folder', type=str, help='path to SSTable folder')
    return parser



if __name__ == "__main__":
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

    server_address = (args.tablet_hostname, args.tablet_port)
    handler_class = MyHandler
    server_class = HTTPServer

    httpd = HTTPServer(server_address, handler_class)
    print("sample server running...")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt: pass

    httpd.server_close()

