from pickle import dumps as pickle, loads as unpickle
from struct import pack, unpack

__all__ = ['sendobj']


def sendobj(sock, obj):
    data = pickle(obj)
    buf = pack('I', len(data)) + data
    sent = 0
    while sent < len(buf):
        part = sock.send(buf[sent:])
        if part == 0:
            raise ConnectionError('Cannot send all {} bytes'.format(len(buf)))
        sent += part
