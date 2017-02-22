from __future__ import print_function

import os
import time
import json

from swift.obj.diskfile import DiskFileRouter
from swift.common.storage_policy import POLICIES
from swift.common.exceptions import DiskFileDeleted, DiskFileError
from swift.common.utils import Timestamp, get_logger

import grpc
from concurrent import futures

import tsync_pb2 as pb2
import tsync_pb2_grpc as pb2_grpc

from common import PORT

from eventlet import tpool
from swift.obj import diskfile
import thread
import threading
import logging
# So swift.common.utils forcibly monkey-patches the logging module to use
# greened versions of threading code.
# NOTE: saving off "orig" values and restoring them after the
# swift.common.utils import only worked when this was the only way/place
# swift.common.utils could get imported.  But it's not now, and it's not
# guaranteed to be anyway, so this is a more robust way to repair it.
logging.threading = threading
logging.thread = thread
logging._lock = threading.RLock()
tpool.set_num_threads = 0
diskfile.tpool_reraise = lambda f, *args, **kwargs: f(*args, **kwargs)


def write_data(path, chunk_iter):
    with open(path, 'w') as fd:
        for chunk in chunk_iter:
            fd.write(chunk.data)


class SyncServicer(pb2_grpc.SyncServicer):

    def __init__(self, root):
        conf = {
            'devices': os.path.abspath(root),
            'mount_check': False,
        }
        logger = get_logger({})
        self.df_router = DiskFileRouter(conf, logger)
        super(SyncServicer, self).__init__()

    def Sync(self, chunk_iter, context):
        logging.debug('ohai!')
        metadata = dict(context.invocation_metadata())
        # TODO: all df params should be protobuf in metadata
        policy = POLICIES[int(metadata.pop('policy_index'))]
        metadata['policy'] = policy
        df_metadata = json.loads(metadata.pop('df_metadata'))
        try:
            df = self.df_router[policy].get_diskfile(**metadata)
        except DiskFileError as e:
            logging.exception('Bad %s %r' % (e, metadata))
            raise

        try:
            with df.open():
                orig_timestamp = df.data_timestamp
        except DiskFileDeleted as e:
            orig_timestamp = e.timestamp
        except DiskFileError:
            orig_timestamp = Timestamp(0)

        req_timestamp = Timestamp(df_metadata['X-Timestamp'])
        if orig_timestamp >= req_timestamp:
            logging.debug('Already have')
            context.cancel()
        else:
            with df.create() as writer:
                for chunk in chunk_iter:
                    writer.write(chunk.data)
                writer.put(df_metadata)
                writer.commit(req_timestamp)
            logging.debug('commit')
        return pb2.Response()


def serve_forever(args):
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_SyncServicer_to_server(
      SyncServicer(args.devices), server)
    server.add_insecure_port('{}:{}'.format(args.bind_ip, args.port))
    server.start()
    print('Listening on {}'.format(PORT))
    while True:
        try:
            time.sleep(60)
        except KeyboardInterrupt:
            print("User quit.")
            server.stop(None)
            return


if __name__ == "__main__":
    serve_forever()
