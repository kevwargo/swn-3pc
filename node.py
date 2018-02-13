import sys, fcntl, os
import time
try:
    from time import monotonic as _time
except ImportError:
    from time import time as _time
from selectors import DefaultSelector as Selector, EVENT_READ
from socket import SOL_SOCKET, SO_REUSEADDR
from socklib import ObjectSocket
from threading import Thread, Condition, RLock
import _thread
from pickle import dumps as pickle, loads as unpickle
from struct import pack, unpack
import random
from traceback import format_exc


MSG_REQUEST = 'request'
MSG_AGREE   = 'agree'
MSG_ABORT   = 'abort'
MSG_PREPARE = 'prepare'
MSG_COMMIT  = 'commit'
MSG_ACK     = 'ack'

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
        self._nodes_cond = Condition(self._lock)
        self._event_thread = Thread(target=self.event_loop, daemon=True)
        self._sel = Selector()
        self._msgbuf = []
        self._message_handlers = {}
        self.nodes = {id: self.local_node}
        self.size = None
        self.timestamp = 0
        self.running = True
        self.controller = None

    def log(self, msg, *args):
        # with open(os.path.join(os.path.dirname(sys.argv[0]), 'output-{}.log'.format(self.local_node.id)), 'a') as f:
        with open(os.path.join(os.path.dirname(sys.argv[0]), 'debug.log'), 'a') as f:
            t = time.time()
            date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t)) + '{:.3f}'.format(t - int(t))[1:]
            if isinstance(msg, str):
                msg = msg.format(*args)
            else:
                msg = str(msg)
            print('{} [{}]: {}'.format(date, self.local_node.id, msg), file=f, flush=True)

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
                except SystemExit:
                    self.log('EXITING')
                    with self._lock:
                        self.running = False
                        self._msg_cond.notify_all()
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
                nodes = data['nodes']
                count = data['total-node-count']
                self.log('Controller: Nodes: {}, total count: {}', nodes, count)
                with self._lock:
                    self.size = count
                    self.log('Setting self.size = {}', self.size)
                    self._nodes_cond.notify_all()
                for id, host, port in nodes:
                    node = Node(self, host, port, id=id)
                    node.connect()
                self._sel.modify(sock, EVENT_READ, {'handler': self.on_control})
                self.controller = sock
            elif 'node-info' in data:
                id, host, port = map(lambda k: data['node-info'][k], ['id', 'host', 'port'])
                with self._lock:
                    self.nodes[id] = Node(self, host, port, sock=sock, id=id)
                    self.log('New node (connection from {}): {} (total nodes: {} {})', sock.getpeername(), id, len(self.nodes), self.nodes)
                    self._sel.modify(sock, EVENT_READ, {'handler': self.on_message, 'node': self.nodes[id]})
                    self._nodes_cond.notify_all()

    def on_message(self, sock, data):
        try:
            msg = self._extract_msg(sock, data)
        except EOFError:
            self.log('Node {} disconnected', data['node'])
            with self._lock:
                del self.nodes[data['node'].id]
            raise
        except OSError as ose:
            self.log('Node {} disconnected due to an error', data['node'])
            raise
        if msg.iscomplete():
            msg = msg.getdata()
            del data['msg']
            if isinstance(msg, dict) and 'timestamp' in msg and 'data' in msg:
                with self._lock:
                    self.timestamp = max(self.timestamp, msg['timestamp']) + 1
                    self.log('Message from {}: {} (ts {}). New timestamp: {}', data['node'].id, msg, msg['timestamp'], self.timestamp)
                    msg['node'] = data['node']
                    for filter, handler in self._message_handlers.items():
                        if filter(msg):
                            handler(msg)
                            return
                    self._msgbuf.append(msg)
                    self._msg_cond.notify_all()

    def on_control(self, sock, data):
        msg = self._extract_msg(sock, data)
        if msg.iscomplete():
            del data['msg']
            msg = msg.getdata()
            self.log('Control message: {}', msg)
            try:
                exec(msg)
            except SystemExit:
                raise
            except:
                pass

    def register(self, node):
        with self._lock:
            self.nodes[node.id] = node
            self._sel.register(node.sock, EVENT_READ, {'handler': self.on_message, 'node': node})
            self.log('New node (connection to {} using {}): {} (total nodes: {} {})', node.sock.getpeername(), node.sock.getsockname(), node.id, len(self.nodes), self.nodes)
            self._nodes_cond.notify_all()

    def send(self, id, msg):
        assert isinstance(msg, dict), 'Message should be dict'
        if id != self.local_node.id:
            with self._lock:
                self.timestamp += 1
                self.nodes[id].sock.sendobj({'timestamp': self.timestamp, 'data': msg})

    def broadcast(self, msg):
        with self._lock:
            self.timestamp += 1
            for n in self.nodes.values():
                if n.id != self.local_node.id:
                    n.sock.sendobj({'timestamp': self.timestamp, 'data': msg})

    def recv(self, filter, timeout=None):
        with self._lock:
            while True:
                if not self.running:
                    self.log('MAIN THREAD TERMINATING')
                    exit()
                for m in self._msgbuf:
                    if filter(m):
                        self._msgbuf.remove(m)
                        if self.controller:
                            try:
                                self.controller.sendobj({
                                    'timestamp': m['timestamp'],
                                    'from': m['node'].id,
                                    'to': self.local_node.id,
                                    'data': m['data']
                                    })
                            except:
                                self.log('Exception while sending message {} to controller: {}', m, format_exc())
                        return m
                if timeout is not None:
                    t = _time()
                if not self._msg_cond.wait(timeout):
                    raise TimeoutError
                if timeout is not None:
                    timeout -= _time() - t
                    if timeout <= 0:
                        raise TimeoutError

    def become_coord(self):
        with self._lock:
            self.timestamp += 1
            self._msgbuf.insert(0, {'node': self.local_node, 'data': {'type': MSG_REQUEST, 'change-state': 'coord'}})
            self._msg_cond.notify_all()

    def set_message_handler(self, filter, handler):
       with self._lock:
           self._message_handlers[filter] = handler

    def clear_message_handler(self, filter):
        with self._lock:
            if filter in self._message_handlers:
                del self._message_handlers[filter]

    def wait_for_init(self):
        with self._lock:
            while self.size is None or len(self.nodes) < self.size:
                self.log('In wait: self.size: {}, len(self.nodes): {}', self.size, len(self.nodes))
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

class SSet(set):

    def add(self, item):
        oldlen = len(self)
        set.add(self, item)
        return oldlen < len(self)

class NodeClass(type):

    stateExceptions = {
        'cohort_init': 'cohort_abort',
        'cohort_wait': 'cohort_abort',
        'cohort_prepared': 'cohort_commit',
        'coord_init': 'coord_abort',
        'coord_wait': 'coord_abort',
        'coord_prepared': {
            'timeout': 'coord_abort',
            'fail': 'coord_commit'
        }
    }

    @classmethod
    def _wrap(cls, state, method):
        def _wrapper(self, *args):
            stateRepr = state.replace('_', ' ').upper()
            self.mqueue.log('Entering state {}', stateRepr)
            timeout_handler = None
            fail_handler = None
            if state in cls.stateExceptions:
                try:
                    if isinstance(cls.stateExceptions[state], dict):
                        timeout_handler = getattr(self, 'state_' + cls.stateExceptions[state]['timeout'])
                        assert callable(timeout_handler), 'Timeout handler {} for {} is not callable'.format(timeout_handler, stateRepr)
                        fail_handler = getattr(self, 'state_' + cls.stateExceptions[state]['fail'])
                    else:
                        fail_handler = getattr(self, 'state_' + cls.stateExceptions[state])
                    assert callable(fail_handler), 'Failure handler {} for {} is not callable'.format(fail_handler, stateRepr)
                except:
                    self.mqueue.log('Exception while looking for error handlers: {}', format_exc())
            try:
                return method(self, *args)
            except SystemExit:
                for n in self.mqueue.nodes.values():
                    n.sock.close()
                raise
            except BaseException as e:
                self.mqueue.log('Exception in state {}: {}', stateRepr, format_exc())
                if isinstance(e, TimeoutError) and callable(timeout_handler):
                    timeout_handler()
                elif callable(fail_handler):
                    fail_handler()
        return _wrapper

    def __new__(cls, name, bases, dict):
        for k, v in dict.items():
            if k.startswith('state_') and isinstance(v, type(lambda: None)):
                dict[k] = cls._wrap(k[6:], v)
        return type.__new__(cls, name, bases, dict)


class Node(metaclass=NodeClass):

    def __init__(self, mqueue, host, port, *, id=None, sock=None):
        self.mqueue = mqueue
        self.host = host
        self.port = port
        self.id = id
        self.sock = sock
        self.is_coord = False
        self.request_response = MSG_AGREE
        self.abort_timestamp = 0
        self.request_timestamp = 0

    def __repr__(self):
        return 'Node [{}] at {}:{}'.format(self.id, self.host, self.port)

    def connect(self):
        self.sock = ObjectSocket()
        self.sock.connect((self.host, self.port))
        self.mqueue.log('Connected to {} ({}:{}) using {}', self.id, self.host, self.port, self.sock.getsockname())
        self.sock.sendobj({'node-info': {a: getattr(self.mqueue.local_node, a) for a in ['id', 'host', 'port']}})
        self.mqueue.register(self)

    def send(self, msg):
        self.mqueue.send(self.id, msg)

    def msg_filter(self, type, node=None, timestamp=None):
        def flt(m):
            if not (isinstance(m['data'], dict) and 'type' in m['data']):
                return False
            if isinstance(type, (tuple, list)):
                if not m['data']['type'] in type:
                    return False
            else:
                if m['data']['type'] != type:
                    return False
            if node and m['node'].id != node.id:
                return False
            if timestamp is not None and m['timestamp'] <= timestamp:
                return False
            return True
        return flt

    def state_cohort_init(self):
        msg = self.mqueue.recv(self.msg_filter(MSG_REQUEST))
        if 'change-state' in msg['data'] and msg['data']['change-state'] == 'coord':
            self.mqueue.log('{} became Coordinator', self)
            return self.state_coord_init()
        self.request_timestamp = msg['timestamp']
        coord = msg['node']
        if self.request_response == MSG_AGREE:
            text = 'agreeing'
        elif self.request_response == MSG_ABORT:
            text = 'aborting'
        else:
            text = 'replying nothing, simulating timeout'
        self.mqueue.log('Received Commit Request from {}, {}', coord, text)
        if self.request_response == MSG_AGREE:
            coord.send({'type': MSG_AGREE})
            return self.state_cohort_wait(coord)
        elif self.request_response == MSG_ABORT:
            coord.send({'type': MSG_ABORT})
            return self.state_cohort_abort()

    def state_cohort_wait(self, coord):
        msg = self.mqueue.recv(self.msg_filter((MSG_PREPARE, MSG_ABORT), node=coord, timestamp=self.request_timestamp), 10)['data']
        if msg['type'] == MSG_PREPARE:
            try:
                coord.send({'type': MSG_ACK})
            except OSError:
                self.mqueue.log('Exception in COHORT WAIT during ACK send: {}', format_exc())
                return self.state_cohort_abort()
            return self.state_cohort_prepared(coord)
        else:
            self.mqueue.log('Received ABORT from Coordinator in COHORT WAIT')
            return self.state_cohort_abort()

    def state_cohort_prepared(self, coord):
        msg = self.mqueue.recv(self.msg_filter((MSG_COMMIT, MSG_ABORT), coord), 10)['data']
        if msg['type'] == MSG_ABORT:
            self.mqueue.log('Received ABORT from Coordinator in COHORT PREPARED')
            return self.state_cohort_abort()
        return self.state_cohort_commit()

    def state_cohort_abort(self):
        return False

    def state_cohort_commit(self):
        return True


    def _bcast_abort(self):
        self.mqueue.log('{} broadcasting ABORT', self)
        try:
            self.mqueue.broadcast({'type': MSG_ABORT})
        except KeyboardInterrupt:
            raise
        except:
            pass

    def state_coord_init(self):
        self.mqueue.log('{} broadcasting REQUEST', self)
        self.mqueue.broadcast({'type': MSG_REQUEST})
        self.state_coord_wait()

    def state_coord_wait(self):
        agrees = SSet()
        while len(agrees) < len(self.mqueue.nodes) - 1:
            msg = self.mqueue.recv(self.msg_filter((MSG_AGREE, MSG_ABORT), timestamp=self.abort_timestamp), 10)
            n = msg['node']
            if msg['data']['type'] == MSG_AGREE:
                self.mqueue.log('Received {} AGREE from {}', ('new' if agrees.add(n.id) else 'repeated'), n)
            else:
                self.mqueue.log('Received ABORT from {}', n)
                return self.state_coord_abort()
        self.mqueue.log('Received AGREEs: {} (n={})', agrees, len(agrees))
        for n in self.mqueue.nodes.values():
            n.send({'type': MSG_PREPARE})
        return self.state_coord_prepared()

    def state_coord_prepared(self):
        acks = SSet()
        while len(acks) < len(self.mqueue.nodes) - 1:
            msg = self.mqueue.recv(self.msg_filter(MSG_ACK), 10)
            n = msg['node']
            self.mqueue.log('Received {} ACK from {}', ('new' if acks.add(n.id) else 'repeated'), n)
        for n in self.mqueue.nodes.values():
            try:
                n.send({'type': MSG_COMMIT})
            except KeyboardInterrupt:
                raise
            except:
                pass
        return self.state_coord_commit()

    def state_coord_abort(self):
        self._bcast_abort()
        self.abort_timestamp = self.mqueue.timestamp
        return False

    def state_coord_commit(self):
        return True

    def loop(self):
        while True:
            self.state_cohort_init()



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
