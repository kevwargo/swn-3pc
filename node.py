import sys, fcntl, os
from time import sleep, asctime
from selectors import DefaultSelector as Selector, EVENT_READ
from socket import socket, SOL_SOCKET, SO_REUSEADDR
from threading import Thread, Condition, RLock
from pickle import dumps as pickle, loads as unpickle
from struct import pack, unpack
from socklib import sendobj


_global_node = None

def log(msg):
    with open(os.path.join(os.path.dirname(sys.argv[0]), 'output-{}.log'.format(os.getpid())), 'a') as f:
        print('{}({}): {}\n'.format(_global_node.id, os.getpid(), msg), file=f, flush=True)

class Message:

    def __init__(self):
        self.lenbuf = b''
        self.len = None
        self.databuf = b''
        self.data = None

    def iscomplete(self):
        return self.data is not None

    def feed(self, sock):
        if not self.len:
            rcvd = sock.recv(4 - len(self.lenbuf))
            if not rcvd:
                raise EOFError
            self.lenbuf += rcvd
            if len(self.lenbuf) == 4:
                self.len = unpack('I', self.lenbuf)[0]
                self.lenbuf = b''
        elif len(self.databuf) < self.len:
            sock.settimeout(0)
            try:
                while len(self.databuf) < self.len:
                    rcvd = sock.recv(self.len - len(self.databuf))
                    if not rcvd:
                        raise EOFError
                    self.databuf += rcvd
                self.data = unpickle(self.databuf)
            except BlockingIOError:
                pass
            finally:
                sock.settimeout(None)

    def getdata(self):
        if not self.iscomplete():
            raise ValueError('Message is not complete')
        return self.data
    

class MQueue:

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._sock = socket()
        self._lock = RLock()
        self._cond = Condition(self._lock)
        self._event_thread = Thread(target=self.event_loop)
        self._sel = Selector()
        self._msgbuf = []
        self._nodes = {}
        self.size = None
        self._env_cond = Condition(self._lock)

    def event_loop(self):
        while True:
            # log('Waiting for event...')
            for key, events in self._sel.select():
                try:
                    # log('Event in event_log: {}'.format(key))
                    key.data['handler'](key.fileobj, key.data)
                except (EOFError, OSError) as e:
                    if not isinstance(e, EOFError):
                        print(e, file=sys.stderr)
                    self._sel.unregister(key.fileobj)

    def connect_handler(self, sock, data):
        c, a = self._sock.accept()
        # log('New connection from {} - {}'.format(a, c))
        self._sel.register(c, EVENT_READ, {'handler': self.raw_handler})

    def _extract_msg(self, sock, data):
        if not 'msg' in data:
            msg = Message()
            data['msg'] = msg
        else:
            msg = data['msg']
        if not msg.iscomplete():
            msg.feed(sock)
        return msg

    def raw_handler(self, sock, data):
        msg = self._extract_msg(sock, data)
        if msg.iscomplete():
            data = msg.getdata()
            log('Message received: {}'.format(data))
            if 'nodes' in data and 'total-node-count' in data:
                log('Coordinator: Nodes: {}, total count: {}'.format(data['nodes'], data['total-node-count']))
                with self._lock:
                    for id, host, port in data['nodes']:
                        node = Node(self, id=id)
                        node.connect(host, port)
                self._sel.unregister(sock)
                sock.close()
                with self._lock:
                    self.size = data['total-node-count']
            elif 'node-id' in data:
                log('Node-id from {}: {}'.format(sock, data['node-id']))
                with self._lock:
                    self._nodes[data['node-id']] = Node(self, sock=sock)
                    self._sel.modify(sock, EVENT_READ, {'handler': self.message_handler, 'node': self._nodes[data['node-id']]})
                    log('receive {} {}'.format(len(self._nodes), self._nodes))
                    self._env_cond.notify_all()

    def message_handler(self, sock, data):
        try:
            msg = self._extract_msg(sock, data)
        except EOFError:
            log('Node {} disconnected'.format(data['node']))
            with self._lock:
                del self._nodes[data['node'].id]
            raise
        except OSError as ose:
            log('Node {} disconnected due to an error'.format(data['node']))
            raise
        if msg.iscomplete():
            with self._lock:
                # log('Message from {}: {}'.format(data['node'].id, msg.getdata()))
                self._msgbuf.append({'node': data['node'], 'data': msg.getdata()})
                self._cond.notify_all()

    def register(self, node):
        with self._lock:
            self._nodes[node.id] = node
            self._sel.register(node.sock, EVENT_READ, {'handler': self.message_handler, 'node': node})
            log('connect {} {}'.format(len(self._nodes), self._nodes))
            self._env_cond.notify_all()

    def send(self, id, msg):
        with self._lock:
            sendobj(self._nodes[id].sock, msg)

    def wait_for_init(self):
        with self._lock:
            while self.size is None or len(self._nodes) < self.size - 1:
                self._env_cond.wait()

    def start(self):
        log('Starting MQueue...')
        self._sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self._sock.bind((self.host, self.port))
        self._sock.listen(5)
        self._sel.register(self._sock, EVENT_READ, {'handler': self.connect_handler})
        self._event_thread.start()
        

class Node:

    def __init__(self, mqueue, *, sock=None, id=None):
        self.mqueue = mqueue
        self.sock = sock
        self.id = id

    def connect(self, host, port):
        log('Connecting to {} ({}:{})'.format(self.id, host, port))
        self.sock = socket()
        self.sock.connect((host, port))
        sendobj(self.sock, {'node-id': self.id})
        self.mqueue.register(self)

    def loop(self):
        while True:
            sleep(10)

def main(id, host, port):
    mqueue = MQueue(host, port)
    print('LISTEN_OK', flush=True)
    global _global_node
    _global_node = Node(mqueue, id=id)
    log('before start...')
    mqueue.start()
    log('waiting for init...')
    mqueue.wait_for_init()
    log(('initialized', mqueue._nodes))
    _global_node.loop()


if __name__ == '__main__':
    main(int(sys.argv[1]), sys.argv[2], int(sys.argv[3]))
