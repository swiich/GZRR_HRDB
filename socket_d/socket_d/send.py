# coding=utf-8

import socket
import datetime
import time
import struct


def socket_send(socketObj, file_path):
    try:
        print('sending...')
        while True:
            with open(file_path, 'rb') as f:
                while True:
                    head, ts, payload = single_frame(f)
                    head_n, ts_n, payload_n = single_frame(f)

                    if not head and not head_n:
                        print('{0} file send over...'.format(file_path))
                        break

                    socketObj.send(head+payload)
                    if not head_n:
                        break
                    # 以每一帧之间监测时间差为间歇发送时间
                    time.sleep(time_delta(ts, ts_n))
                    socketObj.send(head_n+payload_n)
            time.sleep(1)
            socketObj.close()
            print('{0} file send over...'.format(file_path))
            break

    except socket.error as msg:
        print(msg)


def single_frame(opend_f):
    """opend_f参数为打开后的文件对象"""
    head = opend_f.read(24)
    if not head:
        return None, None, None
    else:
        ts = struct.unpack('<H5BH', head[10:19])
        pl, el = struct.unpack('<IB', head[19:])
        return head, ts, opend_f.read(pl+el)


def time_delta(t1, t2):
    """
    t为single_frame()中返回的ts(ts为时间组成的tuple(yyyy,mm,dd,hh,min,millisec))
    :return seconds (float)
    """
    t1 = list(t1)
    t1[-1] *= 1000
    t2 = list(t2)
    t2[-1] *= 1000
    return (datetime.datetime(*t2)-datetime.datetime(*t1)).total_seconds()

