#!/usr/bin/python3

import sys, random, os
from subprocess import Popen, PIPE, call
from threading import Thread, Condition
from socklib import ObjectSocket
from pickle import dumps as pickle, loads as unpickle
from struct import pack, unpack


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
            

def main(procnum, hostnames):
    hosts = {}
    ids = list(range(1, procnum + 1))
    random.shuffle(ids)
    for id in ids:
        host = hostnames[id % len(hostnames)]
        if not host in hosts:
            hosts[host] = {}
        hosts[host][id] = 10000 + len(hosts[host])
    launchers = []
    error = None
    for host, nodes in hosts.items():
        try:
            launchers.append(RemoteLauncher(host, nodes))
        except Exception as e:
            error = e
            break
    if error:
        for launcher in launchers:
            launcher.terminate()
        print(error, file=sys.stderr)
    else:
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
        nodes = []
        for host, info in hosts.items():
            for id, port in info.items():
                nodes.append((id, host, port))
        print(nodes)
        for id, host, port in nodes:
            s = ObjectSocket()
            try:
                s.connect((host, port))
            except ConnectionError as ce:
                print(ce, (host, port))
            msg = {'total-node-count': len(nodes), 'nodes': [n for n in nodes if n[0] < id]}
            print('Sending {} to {} ({}:{})'.format(msg, id, host, port))
            s.sendobj(msg)
            s.close()

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: {} PROC_NUM HOST1 [HOST2 ...]', file=sys.stderr)
        exit(1)
    main(int(sys.argv[1]), sys.argv[2:])
