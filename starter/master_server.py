from flask import Flask, request
import argparse
import os
import os.path as osp

global tablet1
global tablet2
global tablet3

app = Flask(__name__)


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
    parser = get_args_parser()
    args = parser.parse_args()
    tablet1 = {'host': args.tablet1_hostname, 'port': args.tablet1_port}
    tablet2 = {'host': args.tablet2_hostname, 'port': args.tablet2_port}
    tablet3 = {'host': args.tablet3_hostname, 'port': args.tablet3_port}
    app.run(args.master_hostname, args.master_port)
