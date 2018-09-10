from tools.analyse_stream import Read
import os
import numpy as np


class MBasicDataTable(Read):
    def __init__(self, file_name):
        self.file_name = file_name

    def header_payload(self):
        file = open(self.file_name, 'rb')
        while True:
            leader = file.read(4)
            if not leader:                # 判断是否读取到文件末
                break
            file.read(6)
            ts = [np.frombuffer(file.read(2), np.uint16)[0]]
            for i in np.frombuffer(file.read(5), np.uint8):
                ts.append(i)
            ts.append(np.frombuffer(file.read(2), np.uint16)[0])    # 年 月 日 时 分 秒 毫秒
            pl = np.frombuffer(file.read(4), np.uint32)[0]
            el = np.frombuffer(file.read(1), np.uint8)[0]            # 扩展帧长度

            ts_str = "{0}-{1}-{2} {3}:{4}:{5}.{6}".format(*ts)

            if el:
                file.read(el)                        # 扩展帧头ExHeader

            payload = self.get_last_time(file, pl)

            head = (ts_str, payload)
            yield head

        file.close()

    @staticmethod
    def get_last_time(file, payload_len):
        """频谱数据"""
        payload = 0
        while payload_len:

            dt = np.frombuffer(file.read(1), np.uint8)[0]
            dl = np.frombuffer(file.read(4), np.uint32)[0]

            file.read(dl)
            payload = dt

            payload_len -= dl + 5
        return payload


def get_file_info(file):
    inf = next(MBasicDataTable(file).header_payload())
    m_start_time = inf[0]
    m_stop_time = ''
    m_data_type = inf[1]
    file_size = os.path.getsize(file)
    mfid = os.path.basename(file).split('-')[0]
    for f in MBasicDataTable(file).header_payload():
        m_stop_time = f[0]

    result = (m_start_time, m_stop_time, m_data_type, file_size, mfid)
    return result
