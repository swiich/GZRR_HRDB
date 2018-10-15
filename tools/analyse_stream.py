# coding=utf-8

import numpy as np
import struct


class Read:
    def __init__(self, file_name, file_type):
        self.file_name = file_name
        self.file_type = file_type

    def header_payload(self):
        file = open(self.file_name, 'rb')
        while True:
            leader = file.read(4).hex()
            if not leader:                # 判断是否读取到文件末
                break
            ver = np.frombuffer(file.read(2), np.uint8)[0]
            stc, = struct.unpack('I', file.read(4))
            ts = [struct.unpack('H', file.read(2))[0]]
            for i in struct.unpack('5B', file.read(5)):
                ts.append(i)
            ts.append(struct.unpack('H', file.read(2))[0])    # 年 月 日 时 分 秒 毫秒
            pl, = struct.unpack('I', file.read(4))
            el, = struct.unpack('B', file.read(1))            # 扩展帧长度
            ts_str = "{0}-{1}-{2} {3}:{4}:{5}.{6}".format(*ts)
            if el:
                file.read(el)                        # 扩展帧头ExHeader

            func_map = {'spm': self.spm, 'fsc': self.fsc, 'gps': self.position, 'antenna': self.antenna}
            # 传参顺序 file, pl
            payload = func_map[self.file_type](file, pl)

            head = (leader, ver, stc, ts_str, pl, el)
            yield head, payload

        file.close()

    @staticmethod
    def spm(*args):
        """频谱数据"""
        file = args[0]
        payload_len = args[1]
        payload = []
        while payload_len:

            dt = np.frombuffer(file.read(1), np.uint8)[0]
            dl = np.frombuffer(file.read(4), np.uint32)[0]

            number = np.frombuffer(file.read(1), np.uint8)[0]
            freq_total = np.frombuffer(file.read(4), np.uint32)[0]
            start_freq = np.frombuffer(file.read(8), np.float64)[0]
            step = np.frombuffer(file.read(4), np.float32)[0]
            freq_number = np.frombuffer(file.read(4), np.uint32)[0]
            freq_frame_amount = np.frombuffer(file.read(4), np.uint32)[0]
            spectrum = []
            for i in range(freq_frame_amount):
                spectrum.append(np.frombuffer(file.read(2), np.int16)[0])

            tmp = [dt, dl, number, freq_total, start_freq, step, freq_number,
                   freq_frame_amount, spectrum]
            payload.append(tmp)

            payload_len -= dl + 5
        return payload

    @staticmethod
    def fsc(*args):
        """频段扫描数据"""
        file = args[0]
        payload_len = args[1]
        payload = []
        while payload_len:
            dt = np.frombuffer(file.read(1), np.uint8)[0]
            dl = np.frombuffer(file.read(4), np.uint32)[0]

            freq_sec_num = np.frombuffer(file.read(1), np.uint8)[0]
            channels_total = np.frombuffer(file.read(4), np.uint32)[0]
            start_freq = np.frombuffer(file.read(8), np.float64)[0]
            end_freq = np.frombuffer(file.read(8), np.float64)[0]
            start_freq_num = np.frombuffer(file.read(4), np.uint32)[0]
            step = np.frombuffer(file.read(4), np.float32)[0]
            frame_channel_total = np.frombuffer(file.read(4), np.uint32)[0]
            spectrum = []
            for i in range(frame_channel_total):
                spectrum.append(np.frombuffer(file.read(2), np.int16)[0])

            tmp = [dt, dl, freq_sec_num, channels_total, start_freq, end_freq,
                   start_freq_num, step, frame_channel_total, spectrum]
            payload.append(tmp)
            file.read(4)                       # 原始数据文件payload data部分少4个字节,跳过
            payload_len -= dl + 5
        return payload

    @staticmethod
    def position(*args):
        """ gps数据 """
        file = args[0]
        payload_len = args[1]
        payload = []
        while payload_len:
            dt, = struct.unpack('b', file.read(1))
            dl, = struct.unpack('I', file.read(4))

            satellite_count, = struct.unpack('b', file.read(1))
            height, = struct.unpack('h', file.read(2))
            speed, = struct.unpack('H', file.read(2))
            longitude, = struct.unpack('d', file.read(8))
            latitude, = struct.unpack('d', file.read(8))
            declination, = struct.unpack('h', file.read(2))

            tmp = [dt, dl, satellite_count, height, speed, longitude, latitude, declination]
            payload.append(tmp)

            payload_len -= dl + 5
        return payload

    @staticmethod
    def antenna(*args):
        """ 天线因子 """
        file = args[0]
        payload_len = args[1]
        payload = []
        while payload_len:
            dt, = struct.unpack('b', file.read(1))
            dl, = struct.unpack('I', file.read(4))

            freq_band_index, = struct.unpack('B', file.read(1))
            freq_total, = struct.unpack('I', file.read(4))
            start_freq, = struct.unpack('d', file.read(8))
            stop_freq, = struct.unpack('d', file.read(8))
            step, = struct.unpack('f', file.read(4))
            freq_index, = struct.unpack('I', file.read(4))
            freq_count, = struct.unpack('I', file.read(4))
            spectrum = []
            for i in range(freq_count):
                spectrum.append(struct.unpack('h', file.read(2))[0])

            tmp = [dt, dl, freq_band_index, freq_total, start_freq, stop_freq, step, freq_index, freq_count, spectrum]
            payload.append(tmp)

            payload_len -= dl + 5

        return payload




