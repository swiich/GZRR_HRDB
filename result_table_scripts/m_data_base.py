from tools.analyse_stream import Read
import os
import struct
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
        while payload_len:

            dt = np.frombuffer(file.read(1), np.uint8)[0]
            dl = np.frombuffer(file.read(4), np.uint32)[0]

            file.read(dl)
            payload = dt

            payload_len -= dl + 5
        return payload

    # @staticmethod
    # def fsc(file, payload_len):
    #     """频段扫描数据"""
    #     payload = []
    #     while payload_len:
    #         dt = np.frombuffer(file.read(1), np.uint8)[0]
    #         dl = np.frombuffer(file.read(4), np.uint32)[0]
    #
    #         freq_sec_num = np.frombuffer(file.read(1), np.uint8)[0]
    #         channels_total = np.frombuffer(file.read(4), np.uint32)[0]
    #         start_freq = np.frombuffer(file.read(8), np.float64)[0]
    #         end_freq = np.frombuffer(file.read(8), np.float64)[0]
    #         start_freq_num = np.frombuffer(file.read(4), np.uint32)[0]
    #         step = np.frombuffer(file.read(4), np.float32)[0]
    #         frame_channel_total = np.frombuffer(file.read(4), np.uint32)[0]
    #         spectrum = []
    #         for i in range(frame_channel_total):
    #             spectrum.append(np.frombuffer(file.read(2), np.int16)[0])
    #
    #         tmp = (dt, dl, freq_sec_num, channels_total, start_freq, end_freq,
    #                start_freq_num, step, frame_channel_total, spectrum)
    #         payload.append(tmp)
    #         file.read(4)  # 原始数据文件payload data部分少4个字节,跳过
    #         payload_len -= dl + 5
    #     return payload
    #
    # @staticmethod
    # def position(file, payload_len):
    #     """ gps数据 """
    #     payload = []
    #     while payload_len:
    #         dt, = struct.unpack('b', file.read(1))
    #         dl, = struct.unpack('I', file.read(4))
    #
    #         satellite_count, = struct.unpack('b', file.read(1))
    #         height, = struct.unpack('i', file.read(4))
    #         speed, = struct.unpack('I', file.read(4))
    #         longitude, = struct.unpack('d', file.read(8))
    #         latitude, = struct.unpack('d', file.read(8))
    #         declination, = struct.unpack('h', file.read(2))
    #
    #         tmp = (dt, dl, satellite_count, height, speed, longitude, latitude, declination)
    #         payload.append(tmp)
    #
    #         payload_len -= dl + 5
    #     return payload
    #
    # @staticmethod
    # def antenna(file, payload_len):
    #     """ 天线因子 """
    #     payload = []
    #     while payload_len:
    #         dt, = struct.unpack('b', file.read(1))
    #         dl, = struct.unpack('I', file.read(4))
    #
    #         freq_band_index, = struct.unpack('B', file.read(1))
    #         freq_total, = struct.unpack('I', file.read(4))
    #         start_freq, = struct.unpack('d', file.read(8))
    #         stop_freq, = struct.unpack('d', file.read(8))
    #         step, = struct.unpack('f', file.read(4))
    #         freq_index, = struct.unpack('I', file.read(4))
    #         freq_count, = struct.unpack('I', file.read(4))
    #         spectrum = []
    #         for i in range(freq_count):
    #             spectrum.append(struct.unpack('h', file.read(2))[0])
    #
    #         tmp = (
    #         dt, dl, freq_band_index, freq_total, start_freq, stop_freq, step, freq_index, freq_count, spectrum)
    #         payload.append(tmp)
    #
    #         payload_len -= dl + 5
    #
    #     return payload


def get_file_info(file):
    inf = next(MBasicDataTable(file).header_payload())
    m_start_time = inf[0]
    m_data_type = inf[1]
    file_size = os.path.getsize(file)
    mfid = os.path.basename(file).split('-')[0]
    for f in MBasicDataTable(file).header_payload():
        m_stop_time = f[0]

    return (m_start_time, m_stop_time, m_data_type, file_size, mfid)
