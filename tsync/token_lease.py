from Queue import Queue, Empty, Full
import time
import uuid


class TokenLeaser(object):
    """
    Offer tokens to remote actors to enforce limited access to precious
    resources.  Aquire'd tokens can be re-aquired immediately until they
    timeout/expire or are (in some cases forcibly) released.  Attempting to
    aquire a new token will block until an existing token is expired or
    released - up to the timeout.

    It is not the gRPC connections that are precious.  It is the right to use a
    disk.  Allow services to communicate about their state in a lively and open
    fashion - grant access to those actors which need it most.
    """

    def __init__(self, size, expiration=10):
        self.size = size
        self.q = Queue(self.size)
        for i in range(self.size):
            self._fill()
        self.tokens = {}
        self.expiration = expiration

    def _fill(self):
        try:
            self.q.put_nowait(uuid.uuid4().hex)
        except Full:
            pass

    def aquire(self, token=None, timeout=30):
        ttl = self._expire()
        if token is None or token not in self.tokens:
            token = self._wait(timeout, ttl)
        self.tokens[token] = time.time()
        return token

    def _wait(self, timeout, ttl):
        while timeout > 0:
            try:
                return self.q.get(timeout=min(timeout, ttl))
            except Empty:
                pass
            timeout -= ttl
            ttl = self._expire()
        raise Empty()

    def _expire(self):
        ttl = self.expiration
        cutoff = time.time() - self.expiration
        for token, last_used in self.tokens.items():
            if last_used < cutoff:
                self.release(token)
            else:
                ttl = min(ttl, last_used - cutoff)
        return ttl

    def release(self, token):
        self.tokens.pop(token, None)
        self._fill()
