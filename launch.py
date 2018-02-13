#!/usr/bin/python3

import sys, os
from subprocess import Popen, PIPE
from pickle import dumps as pickle, loads as unpickle

def log(msg):
    return
    with open(os.path.join(os.path.dirname(sys.argv[0]), 'launch-{}.log'.format('-'.join(sys.argv[1:]))), 'a') as f:
        print(msg, file=f, flush=True)

def main(nodes_info):
    nodes = []
    for id, host, port in map(lambda n: n.split(':'), nodes_info):
        try:
            args = ['python3', os.path.join(os.path.dirname(sys.argv[0]), 'node.py'), id, host, port]
            log('before {}'.format(['node'] + args[2:]))
            nodes.append((id, Popen(args, stdout=PIPE, stderr=PIPE)))
            log('after {}'.format(['node'] + args[2:]))
        except:
            for i, node in nodes:
                node.terminate()
            break
    pids = []
    for id, node in nodes:
        status = node.stdout.readline().strip()
        log('status from {}: {} {}'.format(['node'] + node.args[2:], status, node.returncode))
        if status == b'LISTEN_OK':
            pids.append(node.pid)
        else:
            if node.returncode != 0:
                errmsg = node.stderr.read().decode('iso-8859-1')
            else:
                errmsg = 'Unrecognized output from node.py (id: {}): {}'.format(id, status)
            for i, n in nodes:
                n.terminate()
            raise OSError(errmsg)
    for pid in pids:
        print(pid)
    log(pids)


if __name__ == '__main__':
    main(sys.argv[1:])
