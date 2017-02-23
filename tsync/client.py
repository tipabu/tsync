from __future__ import print_function
from swift.obj.diskfile import get_data_dir, DiskFileRouter
from swift.common.storage_policy import POLICIES
import thread
import threading
import logging
# So swift.common.utils forcibly monkey-patches the logging module to use
# greened versions of threading code.
# NOTE: saving off "orig" values and restoring them after the
# swift.common.utils import only worked when this was the only way/place
# swift.common.utils could get imported.  But it's not now, and it's not
# guaranteed to be anyway, so this is a more robust way to repair it.
# XXX lp bug #1380815
logging.threading = threading
logging.thread = thread
logging._lock = threading.RLock()

import errno
import os
from Queue import Queue, Empty
from threading import Thread
import time
from multiprocessing import Process
import json


import grpc

import tsync_pb2 as pb2
import tsync_pb2_grpc as pb2_grpc


def join_list(root, item):
    path = os.path.join(root, item)
    try:
        files = os.listdir(path)
    except OSError as e:
        if e.errno not in (errno.ENOTDIR, errno.ENOENT):
            raise
        files = []
    return path, files


def iter_parts(args, device):
    device_path = os.path.join(args.devices, device)
    policy = POLICIES[args.policy_index]
    datadir = get_data_dir(policy)
    logging.debug('getting parts from %r %r', device_path, datadir)
    datadir_path, parts = join_list(device_path, datadir)
    for part in parts:
        try:
            part = int(part)
        except ValueError:
            continue
        sync_to = [n for n in policy.object_ring.get_part_nodes(part)
                   if n['device'] != device]
        yield {
            'device': device,
            'part': part,
            'policy': policy,
            'datadir_path': datadir_path,
            'sync_to': sync_to,
        }


def iter_hashes(device, datadir_path, part, **kwargs):
    part_path, suffs = join_list(datadir_path, str(part))
    for suff in suffs:
        suff_path, hashes = join_list(part_path, suff)
        for hash_ in hashes:
            yield hash_


def chunk_iterator(reader):
    for chunk in reader:
        yield pb2.Chunk(data=chunk)


def consume(q):
    """
    This guy eats results from TSync server - he would also be the guy to
    notice pre-emption (a primary sync rejected in-favor of a handoff revert)

    There's no way to get around that he needs to signal back to the mainthread
    when/if it should stop consuming from the queue.  This is where channels
    making it super convenient to do do CSP right.  In Python we'd probably use
    a condition or an event - or just make a queue and put in a "Hey shut
    things down we've hit too-many/bad errors or like a 420 enhance your calm"
    """
    batch = []
    while True:
        while len(batch) < 10:
            try:
                f = q.get_nowait()
            except Empty:
                time.sleep(0.1)
                continue
            if f is None:
                return
            batch.append(f)
        for f in list(batch):
            if f.done():
                try:
                    f.result()
                except Exception as e:
                    logging.warning(e)
                batch.remove(f)


def sync_parts(args, stub, df_mgr, feeder_q, finished_q):
    while True:
        part_info = feeder_q.get()
        if part_info is None:
            logging.info('Thread is shutting down')
            return
        logging.debug('going to sync part %r' % (part_info,))
        for hash_ in iter_hashes(**part_info):
            logging.debug('going to sync hash_ %r', hash_)
            df = df_mgr.get_diskfile_from_hash(
                part_info['device'],
                part_info['part'],
                hash_,
                part_info['policy']
            )
            with df.open():
                logging.debug('opened %r', df)
                # XXX this should be a protobuf message (yes stuffed into the headers)
                # https://github.com/grpc/grpc-java/blob/v1.1.2/protobuf/src/main/java/io/grpc/protobuf/ProtoUtils.java#L124
                metadata = {
                    'df_metadata': json.dumps(df.get_metadata()),
                    'policy_index': str(int(part_info['policy'])),
                    'device': None,  # set per node
                    'partition': str(part_info['part']),
                    'account': df._account,
                    'container': df._container,
                    'obj': df._obj,
                }
                for node in part_info['sync_to']:
                    metadata['device'] = node['device']
                    # TODO: pull per-node stub from pool, reusing tcp
                    # connections from a pool is brilliant - only connecting to
                    # one node is cheating
                    logging.info('sending %r' % metadata)
                    f = stub.Sync.future(chunk_iterator(df.reader()),
                                         metadata=metadata.items())
                    finished_q.put(f)


def sync_device(args, device):
    logging.info('worker started!')
    policy = POLICIES[args.policy_index]
    conf = {
        'devices': args.devices,
        'mount_check': False,
    }
    df_mgr = DiskFileRouter(conf, logging)[policy]
    logging.debug('connecting')
    channel = grpc.insecure_channel('{}:{}'.format(args.bind_ip, args.port))
    stub = pb2_grpc.SyncStub(channel)
    logging.debug('connected')
    feeder_q = Queue(args.threads_per_dev * 10)
    finished_q = Queue(args.threads_per_dev * 10)
    consumer = Thread(target=consume, args=(finished_q,))
    consumer.start()
    workers = []
    for i in range(args.threads_per_dev):
        t = Thread(target=sync_parts, args=(
            args, stub, df_mgr, feeder_q, finished_q))
        t.start()
        workers.append(t)
    logging.debug('feeding queue')
    try:
        for part_info in iter_parts(args, device):
            logging.debug('doing part_info %r', part_info)
            feeder_q.put(part_info)
    finally:
        for t in workers:
            feeder_q.put(None)
        for t in workers:
            t.join()
        finished_q.put(None)
        consumer.join()


def sync(args):
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    workers = []
    policy = POLICIES[args.policy_index]
    policy.load_ring('/etc/swift')
    devices = set([d for d in os.listdir(args.devices) if
                   os.path.isdir(os.path.join(args.devices, d))])

    workers = []
    for dev in devices:
        p = Process(target=sync_device, args=(args, dev))
        p.start()
        workers.append(p)
    for p in workers:
        p.join()
    logging.info('all workers finished')
