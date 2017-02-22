from argparse import ArgumentParser
from collections import defaultdict
import hashlib
import logging
from multiprocessing import Process, Queue
import os
from Queue import Full
import random
import sys
import time

from swift.obj.diskfile import DiskFileRouter
from swift.common.storage_policy import POLICIES
from swift.common.utils import Timestamp, drop_privileges


parser = ArgumentParser()
parser.add_argument('-v', '--verbose', help='debug output',
                    default=False, action='store_true')
parser.add_argument('--policy-index', type=int, default=0,
                    help='specify the polciy index')
parser.add_argument('--devices', default='/srv/node',
                    help='root of devices tree for node')
parser.add_argument('--workers', default=1, type=int,
                    help='number of fillers PER DEVICE')
parser.add_argument('--count', default=100, type=int,
                    help='number of objects')
parser.add_argument('--size', default=100000, type=int,
                    help='number of bytes of object')
parser.add_argument('--account', default='AUTH_test',
                    help='account name')
parser.add_argument('--container', default='filler_test',
                    help="container name (there's no container updates)")
parser.add_argument('--user', default=None,
                    help='If you want to be you be you')


BUFFER = '\x00' * 64 * 2 ** 10


def filler(args, q):
    if args.verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logging.basicConfig(level=level)
    policy = POLICIES[args.policy_index]
    conf = {
        'devices': args.devices,
        'mount_check': False,
    }
    df_mgr = DiskFileRouter(conf, logging)[policy]
    if args.user:
        drop_privileges(args.user)
    while True:
        df_spec = q.get()
        if df_spec is None:
            return
        df = df_mgr.get_diskfile(
            df_spec['device'],
            df_spec['part'],
            df_spec['account'],
            df_spec['container'],
            df_spec['obj'],
            POLICIES[df_spec['policy_index']],
        )
        with df.create() as writer:
            timestamp = Timestamp(time.time())
            remaining = args.size
            hasher = hashlib.md5()
            while remaining > 0:
                chunk = BUFFER[:remaining]
                writer.write(chunk)
                hasher.update(chunk)
                remaining -= len(chunk)
            metadata = {
                'ETag': hasher.hexdigest(),
                'X-Timestamp': timestamp.internal,
                'Content-Length': str(args.size),
            }
            writer.put(metadata)
            writer.commit(timestamp)
        logging.debug(
            'created '
            '/%(device)s/%(part)s/%(account)s/%(container)s/%(obj)s',
            df_spec)


def pick_part_node(args, ring, account, container, obj):
    """
    This assumes every device key is unique by name (like it is on a
    saio or a SwiftStack node), if you use non-unique device names - it
    sucks to be you^W^W^W^Wmay require whataremyips and bind_ip plumbing
    """
    part, nodes = ring.get_nodes(account, container, obj)
    random.shuffle(nodes)
    for node in nodes:
        device_path = os.path.join(args.devices, node['device'])
        if os.path.isdir(device_path):
            return part, node
    for node in ring.get_more_nodes(part):
        device_path = os.path.join(args.devices, node['device'])
        if os.path.isdir(device_path):
            return part, node
    raise Exception('Unable to find local device in %s' % args.devices)


def make_diskfile(args, i, policy):
    account = args.account
    container = args.container
    obj = 'obj%06x' % i
    part, node = pick_part_node(args, policy.object_ring,
                                account, container, obj)
    return {
        'device': node['device'],
        'part': part,
        'account': account,
        'container': container,
        'obj': obj,
        'policy_index': int(policy)
    }


class DiskQueue(object):

    def __init__(self, devices, workers_per_disk=1):
        self.q = {d: Queue(100) for d in devices}
        self.workers = defaultdict(list)
        self.workers_per_disk = workers_per_disk

    def submit(self, job_spec):
        device = job_spec['device']
        try:
            self.q[device].put_nowait(job_spec)
        except Full:
            def emptyness(q):
                return (1.0 * q._maxsize - q.qsize()) / q._maxsize
            if any(emptyness(q) > 0.9 for q in self.q.values()):
                logging.warning({d: emptyness(q)
                                 for d, q in self.q.items()})
                raise
            else:
                self.q[device].put(job_spec)

    def start(self, target, *args):
        for d, q in self.q.items():
            p_args = list(args)
            p_args.append(q)
            for i in range(self.workers_per_disk):
                p = Process(target=target, args=tuple(p_args))
                self.workers[d].append(p)
                p.start()

    def stop(self):
        for device, workers in self.workers.items():
            for p in workers:
                self.q[device].put(None)

    def join(self):
        for device, workers in self.workers.items():
            for p in workers:
                p.join()


def main():
    args = parser.parse_args()
    policy = POLICIES[args.policy_index]
    policy.load_ring('/etc/swift')
    devices = set([d for d in os.listdir(args.devices) if
                   os.path.isdir(os.path.join(args.devices, d))])
    for dev in policy.object_ring.devs:
        if not dev:
            continue
        if not dev['weight']:
            devices.discard(dev['device'])
    q = DiskQueue(devices, workers_per_disk=args.workers)
    q.start(filler, args)
    dropped = defaultdict(int)
    try:
        remaining = args.count
        i = 0
        while remaining > 0:
            df_spec = make_diskfile(args, i, policy)
            i += 1
            try:
                q.submit(df_spec)
            except Full:
                d = df_spec['device']
                dropped[d] += 1
                logging.warning('Dropped job for %s => %s', d, dropped)
                continue
            remaining -= 1
    finally:
        q.stop()
    q.join()


if __name__ == "__main__":
    sys.exit(main())
