#!/usr/bin/python3

import sys, random, os
from subprocess import Popen, PIPE, call
from threading import Thread, Condition
from socklib import ObjectSocket
from pickle import dumps as pickle, loads as unpickle
from struct import pack, unpack
from traceback import format_exc
import re, readline
from selectors import DefaultSelector as Selector, EVENT_READ
from node import Message

class RemoteLauncher(Popen):

    def __init__(self, host, nodes):
        self.host = host
        self.nodes = nodes
        self.out = None
        self.err = None
        self._pids = None
        self._cond = Condition()
        self._pids_thread = Thread(target=self._pids_thread_target)
        args = ['ssh', host, 'python3', os.path.join(os.getcwd(), 'launch.py')]
        args.extend(['{}:{}:{}'.format(i, host, p) for i, p in nodes.items()])
        Popen.__init__(self, args, stdout=PIPE, stderr=PIPE)
        self._pids_thread.start()

    def get_status(self):
        if self.returncode is None:
            return 'launching'
        elif self._pids:
            return 'launchers PIDs: {}'.format(self._pids)
        else:
            return 'failed with code {}: {}'.format(self.returncode, self.err)

    def __repr__(self):
        return 'RemoteLauncher(host: {}, nodes: {}, status: {})'.format(self.args, self.nodes, self.get_status())
            

    def _pids_thread_target(self):
        self.out, self.err = self.communicate()
        with self._cond:
            if self.returncode == 0:
                try:
                    self._pids = [int(s) for s in self.out.split() if s.strip()]
                except:
                    pass
            self._cond.notify_all()
        
    def getpids(self):
        with self._cond:
            while (self._pids, self.returncode) == (None, None):
                self._cond.wait()
            if self.returncode != 0:
                raise OSError('Process {} exited with code {}: \n{}'.format(self.args, self.returncode, self.err.decode('iso-8859-1')))
            if self._pids is None:
                raise OSError('Unable to parse pids from process output: {}'.format(self.out))
            return self._pids

    def terminate(self):
        print('Terminating ' + self)
        try:
            if self.returncode is None:
                super().terminate()
            elif self.returncode == 0:
                args = ['ssh', host, 'kill']
                args.extend(map(str, self.getpids()))
                call(args)
        except:
            pass


def init_nodes(hosts):
    launchers = []
    for host, nodes in hosts.items():
        try:
            launchers.append(RemoteLauncher(host, nodes))
        except:
            for launcher in launchers:
                launcher.terminate()
            raise
    try:
        for launcher in launchers:
            print(launcher, launcher.getpids())
    except:
        try:
            for launcher in launchers:
                launcher.terminate()
        except:
            pass
        raise
    nodes = {}
    for host, info in hosts.items():
        for id, port in info.items():
            nodes[id] = {'host': host, 'port': port}
    print(nodes)
    for id, node in nodes.items():
        host, port = node['host'], node['port']
        s = ObjectSocket()
        try:
            s.connect((host, port))
        except ConnectionError as ce:
            print(ce, (host, port))
        msg = {'total-node-count': len(nodes), 'nodes': [(i, n['host'], n['port']) for i, n in nodes.items() if i < id]}
        print('Sending {} to {} ({}:{})'.format(msg, id, host, port))
        s.sendobj(msg)
        node['sock'] = s
    return nodes


class MessageLog:

    def __init__(self, nodes):
        self.nodes = nodes
        self.log = []

    def start(self):
        _sel = Selector()
        for n in self.nodes.values():
            _sel.register(n['sock'], EVENT_READ, {})
        Thread(target=self._loop, args=(_sel,), daemon=True).start()

    def _loop(self, sel):
        while True:
            for key, events in sel.select():
                if not 'msg' in key.data:
                    msg = Message()
                    key.data['msg'] = msg
                else:
                    msg = key.data['msg']
                if not msg.iscomplete():
                    msg.feed(key.fileobj)
                # print('msg')
                if msg.iscomplete():
                    # print('msg complete')
                    del key.data['msg']
                    msg = msg.getdata()
                    if isinstance(msg, dict) and 'timestamp' in msg:
                        self.log.append(msg)

    def show(self):
        # print('len ' + str(len(self.log)))
        for i in sorted(self.log, key=lambda l: l['timestamp']):
            print('{} -> {}: {} ({}) {}'.format(i['from'], i['to'], i['data'], i['timestamp'], 'in-channel' if i['in-channel'] else 'delivered'))
                
    
def command_loop(nodes):
    msglog = MessageLog(nodes)
    msglog.start()
    regex = re.compile('^([0-9]+)\s+(.*)')
    while True:
        try:
            string = input('-> ')
        except (EOFError, KeyboardInterrupt):
            for n in nodes.values():
                n['sock'].sendobj('exit()')
            return
        m = regex.match(string)
        if m:
            nodes[int(m.group(1))]['sock'].sendobj(m.group(2))
        elif string.strip() == 'msglog':
            msglog.show()

def main(procnum, hostnames):
    hosts = {}
    ids = list(range(1, procnum + 1))
    random.shuffle(ids)
    for id in ids:
        host = hostnames[id % len(hostnames)]
        if not host in hosts:
            hosts[host] = {}
        hosts[host][id] = 10000 + len(hosts[host])
    nodes = init_nodes(hosts)
    command_loop(nodes)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: {} PROC_NUM HOST1 [HOST2 ...]', file=sys.stderr)
        exit(1)
    main(int(sys.argv[1]), sys.argv[2:])
