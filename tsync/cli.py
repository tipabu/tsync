from __future__ import print_function
from argparse import ArgumentParser

from server import serve_forever
from client import sync
import common

action_map = {
    'server': serve_forever,
    'sync': sync,
}

parser = ArgumentParser()
parser.add_argument('cmd', choices=action_map.keys())
parser.add_argument('--verbose', action='store_true',
                    help='moar logging')
parser.add_argument('--bind-ip', default='[::]')
parser.add_argument('--port', default=common.PORT)
parser.add_argument('--policy-index', default=0, type=int,
                    help='set policy index')
parser.add_argument('--devices', default='/srv/node',
                    help='set devices root')
parser.add_argument('--threads-per-dev', default=4, type=int,
                    help='set number of threads per device')


def main():
    args = parser.parse_args()
    return action_map[args.cmd](args)

if __name__ == '__main__':
    main()

