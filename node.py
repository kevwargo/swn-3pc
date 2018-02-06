import sys, fcntl, os
from time import sleep, asctime
from selectors import DefaultSelector as Selector, EVENT_READ
from socket import SOL_SOCKET, SO_REUSEADDR
from socklib import ObjectSocket
from threading import Thread, Condition, RLock
from pickle import dumps as pickle, loads as unpickle
from struct import pack, unpack
from random import randint as random
from traceback import format_exc


class Message:

    def __init__(self):
        self.lenbuf = b''
        self.len = None
        self.databuf = b''
        self.data = None

    def __repr__(self):
        if self.len is None:
            return 'Message ({} of length)'.format(len(self.lenbuf))
        if self.data is None:
            return 'Message ({} of {} received)'.format(len(self.databuf), self.len)
        return 'Message: {}'.format(self.data)

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

    def __init__(self, id, host, port):
        self.local_node = Node(self, host, port, id=id)
        self._sock = ObjectSocket()
        self._lock = RLock()
        self._msg_cond = Condition(self._lock)
        self._event_thread = Thread(target=self.event_loop)
        self._sel = Selector()
        self._msgbuf = []
        self._nodes = {id: self.local_node}
        self.size = None
        self._nodes_cond = Condition(self._lock)
        self.timestamp = 0

    def log(self, msg, *args):
        with open(os.path.join(os.path.dirname(sys.argv[0]), 'output-{}.log'.format(self.local_node.id)), 'a') as f:
            print('{}({}): {}'.format(self.local_node.id, os.getpid(), msg.format(*args)), file=f, flush=True)

    def event_loop(self):
        while True:
            # self.log('Waiting for event...')
            for key, events in self._sel.select():
                try:
                    # self.log('Event in event_log: {}', key)
                    if 'handler' in key.data:
                        key.data['handler'](key.fileobj, key.data)
                except EOFError:
                    self._sel.unregister(key.fileobj)
                except:
                    self.log('Exception in event_loop (key: {}): {}', key, format_exc())
                    self._sel.unregister(key.fileobj)

    def on_connection(self, sock, data):
        c, a = self._sock.accept()
        self.log('New connection from {}', a)
        self._sel.register(c, EVENT_READ, {'handler': self.on_raw_data})

    def _extract_msg(self, sock, data):
        if not 'msg' in data:
            msg = Message()
            data['msg'] = msg
        else:
            msg = data['msg']
        if not msg.iscomplete():
            msg.feed(sock)
        return msg

    def on_raw_data(self, sock, data):
        msg = self._extract_msg(sock, data)
        if msg.iscomplete():
            data = msg.getdata()
            self.log('Message received: {} from {}', data, sock.getpeername())
            if 'nodes' in data and 'total-node-count' in data:
                self._sel.unregister(sock)
                sock.close()
                nodes = data['nodes']
                count = data['total-node-count']
                self.log('Coordinator: Nodes: {}, total count: {}', nodes, count)
                with self._lock:
                    self.size = count
                    self.log('Setting self.size = {}', self.size)
                    self._nodes_cond.notify_all()
                for id, host, port in nodes:
                    node = Node(self, host, port, id=id)
                    node.connect()
            elif 'node-info' in data:
                id, host, port = map(lambda k: data['node-info'][k], ['id', 'host', 'port'])
                with self._lock:
                    self._nodes[id] = Node(self, host, port, sock=sock, id=id)
                    self.log('New node (connection from {}): {} (total nodes: {} {})', sock.getpeername(), id, len(self._nodes), self._nodes)
                    self._sel.modify(sock, EVENT_READ, {'handler': self.on_message, 'node': self._nodes[id]})
                    self._nodes_cond.notify_all()

    def on_message(self, sock, data):
        try:
            msg = self._extract_msg(sock, data)
        except EOFError:
            self.log('Node {} disconnected', data['node'])
            with self._lock:
                del self._nodes[data['node'].id]
            raise
        except OSError as ose:
            self.log('Node {} disconnected due to an error', data['node'])
            raise
        if msg.iscomplete():
            msg = msg.getdata()
            del data['msg']
            if isinstance(msg, dict) and 'timestamp' in msg and 'data' in msg:
                timestamp, msg = msg['timestamp'], msg['data']
                with self._lock:
                    self.timestamp = max(self.timestamp, timestamp) + 1
                    self.log('Message from {}: {}. New timestamp: {}', data['node'].id, msg, self.timestamp)
                    self._msgbuf.append({'node': data['node'], 'data': msg})
                    self._msg_cond.notify_all()

    def register(self, node):
        with self._lock:
            self._nodes[node.id] = node
            self._sel.register(node.sock, EVENT_READ, {'handler': self.on_message, 'node': node})
            self.log('New node (connection to {} using {}): {} (total nodes: {} {})', node.sock.getpeername(), node.sock.getsockname(), node.id, len(self._nodes), self._nodes)
            self._nodes_cond.notify_all()

    def send(self, id, msg):
        if id != self.local_node.id:
            with self._lock:
                self.timestamp += 1
                self._nodes[id].sock.sendobj({'timestamp': self.timestamp, 'data': msg})

    def wait_for_init(self):
        with self._lock:
            while self.size is None or len(self._nodes) < self.size:
                self.log('In wait: self.size: {}, len(self._nodes): {}', self.size, len(self._nodes))
                self._nodes_cond.wait()

    def start(self):
        # self.log('Starting MQueue...')
        self._sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self._sock.bind((self.local_node.host, self.local_node.port))
        self._sock.listen(5)
        self._sel.register(self._sock, EVENT_READ, {'handler': self.on_connection})
        self._event_thread.start()

    def loop(self):
        try:
            self.local_node.loop()
        except:
            self.mqueue.log('Exception in the main loop: {}', format_exc())
        

class Node:

    def __init__(self, mqueue, host, port, *, id=None, sock=None):
        self.mqueue = mqueue
        self.host = host
        self.port = port
        self.id = id
        self.sock = sock

    def __repr__(self):
        return 'Node [{}] at {}:{}'.format(self.id, self.host, self.port)

    def connect(self):
        self.sock = ObjectSocket()
        self.sock.connect((self.host, self.port))
        self.mqueue.log('Connected to {} ({}:{}) using {}', self.id, self.host, self.port, self.sock.getsockname())
        self.sock.sendobj({'node-info': {a: getattr(self.mqueue.local_node, a) for a in ['id', 'host', 'port']}})
        self.mqueue.register(self)

    def loop(self):
        while True:
            sleep(random(3, 10))
            id = random(1, self.mqueue.size)
            self.mqueue.send(id, 'Random {} from {} to {}'.format(random(1, 1000000), self.id, id))




def main(id, host, port):
    mqueue = MQueue(id, host, port)
    mqueue.log('Starting node {} at {}:{}', id, host, port)
    mqueue.start()
    print('LISTEN_OK', flush=True)
    mqueue.log('waiting for init...')
    mqueue.wait_for_init()
    mqueue.log('STARTING MAIN LOOP...')
    mqueue.loop()


if __name__ == '__main__':
    main(int(sys.argv[1]), sys.argv[2], int(sys.argv[3]))
