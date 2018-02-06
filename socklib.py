from pickle import dumps as pickle, loads as unpickle
from struct import pack, unpack
from socket import socket

__all__ = ['ObjectSocket']


class ObjectSocket(socket):

    def __init__(self, _sock=None):
        if _sock is not None:
            self._sock = _sock
        else:
            self._sock = socket()

    def __getattribute__(self, attr):
        if not attr in ('_sock', 'accept', 'sendobj'):
            return socket.__getattribute__(self._sock, attr)
        else:
            return object.__getattribute__(self, attr)

    def __setattr__(self, attr, value):
        if not attr in ('_sock'):
            return socket.__setattr__(self._sock, attr, value)
        else:
            return object.__setattr__(self, attr, value)

    def sendobj(self, obj):
        data = pickle(obj)
        buf = pack('I', len(data)) + data
        sent = 0
        while sent < len(buf):
            part = self.send(buf[sent:])
            if part == 0:
                raise ConnectionError('Cannot send all {} bytes'.format(len(buf)))
            sent += part

    def accept(self):
        c, a = self._sock.accept()
        return type(self)(c), a
