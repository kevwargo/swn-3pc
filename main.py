from mpi4py import MPI
import netifaces
from time import sleep

if __name__ == '__main__':
    comm = MPI.COMM_WORLD
    rank = comm.rank
    size = comm.size
    print('rank %d of size %d (IP: %s)' % (rank, size, netifaces.ifaddresses('eth0')[netifaces.AF_INET][0]['addr']))
    # with open('/proc/net/tcp') as f:
    #     for l in f:
    #         if '08AE' in l:
    #             print(rank, l)
