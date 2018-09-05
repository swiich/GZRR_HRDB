import socket
import threading
import struct
import sys
from py_hdfs import py_hdfs
from hive import hive_connector
from socket_d import send
import os


def socket_recv():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('172.39.8.62', 10001))
        s.listen(10)

        print('waiting connection...')

        while True:
            conn, addr = s.accept()

            t = threading.Thread(target=deal_data, args=(conn, addr))
            t.start()
    except socket.error as msg:
        print(msg)
    except KeyboardInterrupt:
        print('user interrupts...')
        sys.exit(0)


def deal_data(conn, addr):
    print('accept new connection from {0}'.format(addr))

    try:
        buf = conn.recv(24)
        if buf:
            el, = struct.unpack('b', buf[23:24])
            buf = conn.recv(el)
            dataguid, = struct.unpack('%ss' % el, buf)
            # print(dataguid)
            # print(el)
            print('receive ended...')
            cursor = hive_connector.get_hive_cursor('172.39.8.60', 'db_data_store')
            sql = 'select file_location from file_index where dataguid=="%s"' % str(dataguid.decode())
            result = hive_connector.execute_sql(cursor, sql)[0][0]
            file_local = './py_hdfs/tmp/%s' % os.path.basename(result)
            # 如果本地没有文件缓存则从HDFS上下载
            if not os.path.exists(file_local):
                py_hdfs.download_file(result, file_local)
                print('No local cache... downloading file from hdfs to local...')
            print('send file from local cache...')
            send.socket_send(conn, file_local)
        conn.close()

    except IndexError:
        print("I can't find any file with current dataguid !")
    except ConnectionResetError as msg:
        print(msg)


if __name__ == '__main__':
    socket_recv()

