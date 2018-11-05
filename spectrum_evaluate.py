# coding=utf-8

from struct import unpack
import os
from tools.c_lib.c_invoker import CInvoker
import numpy as np
from math import radians, cos, sin, asin, sqrt
from tools import traverse_file
import time
# import hive_connector as hc


class SpectrumStatistics:
    """
    解析频谱评估数据文件
    """
    def __init__(self, file):
        self.filepath = file
        f_name = os.path.basename(file).split('_')
        self.file = open(file, 'rb')
        self.mscode = f_name[0]
        self.mfcode = f_name[1]
        # self.file_create_time = f_name[2] + f_name[3]
        self.start_freq = float(f_name[4].split('M')[0])*1000000
        self.stop_freq = float(f_name[5].split('M')[0])*1000000
        self.step = float(f_name[6].split('k')[0])*1000
        self.p_mode = f_name[7]
        self.flag = f_name[-1][0]

    def resolve(self):
        file = self.file
        while True:
            leader = file.read(4).hex()
            if not leader:
                break
            equid = unpack('2b', file.read(2))
            device_inf, = unpack('b', file.read(1))
            time_ = unpack('<H5BH', file.read(9))
            scan_rate, = unpack('h', file.read(2))
            longitude, latitude = unpack('2q', file.read(16))
            antenna_height, = unpack('h', file.read(2))
            # start_freq, = unpack('d', file.read(8))
            # step, = unpack('f', file.read(4))
            file.read(12)
            fp_count, = unpack('i', file.read(4))

            time_str = "{0}-{1}-{2} {3}:{4}:{5}.{6}".format(*time_)
            data = []
            for i in range(fp_count):
                data.append(unpack('h', file.read(2))[0])

            res = (self.mscode, self.mfcode, self.start_freq, self.stop_freq,
                   self.step, self.p_mode, self.flag, leader, str(equid[0])+str(equid[1]),
                   device_inf, time_str, scan_rate, longitude, latitude, antenna_height,
                   fp_count)
            yield res, data
        file.close()

    def write_to_file(self, des_path):
        """ 解析结果存入数据文件 """
        file_res = open(des_path, 'a')
        for h, d in self.resolve():
            for i in h:
                file_res.write(str(i)+'|')
            for i, v in enumerate(d):
                if i != len(d)-1:
                    file_res.write(str(v)+',')
                else:
                    file_res.write(str(v))
            file_res.write('\n')
        file_res.close()
        print(self.file.name)

    def monitoring_facility_data_file(self):
        """
        将固定站数据每个文件合并为一帧
        """
        # 将台站库存入内存，避免多次查询
        # cursor = hc.get_hive_cursor('172.18.140.8', 'spectrum_evaluation')
        # sql = "select stat_lg,stat_la,st_serv_r,freqregion,staid from station"
        # station = hc.execute_sql(cursor, sql)

        first_frame = next(self.resolve())
        time_str = time.strptime(first_frame[0][10], '%Y-%m-%d %H:%M:%S.%f')
        date = str(time_str.tm_year)+'-'+str(time_str.tm_mon)+'-'+str(time_str.tm_mday)
        longitude = first_frame[0][-4]
        mfid = str(first_frame[0][0]) + '00' + str(first_frame[0][1])
        latitude = first_frame[0][-3]
        data_len = first_frame[0][-1]
        tmp = np.zeros(data_len)
        scan_count = 0
        amp_dict = {}
        for i in self.resolve():
            stop_freq = i[0][3]
            start_freq = i[0][2]
            step = i[0][4]
            fp_data = i[1]
            # # 计算电平值出现次数
            # for j in fp_data:
            #     if j/10 not in amp_dict.keys():
            #         amp_dict.update({j/10: 1})
            #     else:
            #         amp_dict[j/10] += 1

            occupancy = ocy(fp_data, start_freq, stop_freq, step)
            tmp += occupancy
            scan_count += 1

        index = freq_band_index_split(int(start_freq), int(stop_freq), int(step))
        t = dict(zip(index/1000000, tmp))
        freqregion = str(int(self.start_freq/1000000))+'-'+str(int(self.stop_freq/1000000))
        activepoint = freq_band_split(t, int(freqregion.split('-')[0]), int(freqregion.split('-')[1]))
        with open('/storage/sdb/data/facility', 'a') as f:
            f.write(mfid+'|'+str(longitude) + '|' + str(latitude) + '|' + date + '|' + str(scan_count) + '|' + str(activepoint) + '|' + str(amp_dict))
            f.write('\n')
        # # 匹配台站
        # for j in station:
        #     if haversine(longitude / 100000000, latitude / 100000000, j[0], j[1]) < j[2]:
        #         guid = uuid.uuid1()
        #         freqregion = j[3]
        #         staid = j[4]
        #         activepoint = freq_band_split(t, int(freqregion.split('-')[0]), int(freqregion.split('-')[1]))
        #         with open('facility', 'a') as f:
        #             f.write(str(guid)+'|'+freqregion+'|'+staid+'|'+date+'|'+str(scan_count)+'|'+str(activepoint))
        #             f.write('\n')

        return 1

    def monitoring_car_data_min(self):
        """
        将监测车数据每分钟合并为一帧
        """
        # 将台站库存入内存，避免多次查询
        # cursor = hc.get_hive_cursor('172.18.140.8', 'spectrum_evaluation')
        # sql = "select stat_lg,stat_la,st_serv_r,freqregion,staid from station"
        # station = hc.execute_sql(cursor, sql)

        scan_count = 0
        first_frame = next(self.resolve())
        mfid = str(first_frame[0][0]) + '00' + str(first_frame[0][1])
        fp_data_total = np.zeros(first_frame[0][-1])
        cvg_data = np.zeros(first_frame[0][-1])
        time_min = time.strptime(first_frame[0][10], '%Y-%m-%d %H:%M:%S.%f').tm_min
        amp_dict = {}
        for i in self.resolve():
            time_current = time.strptime(i[0][10], '%Y-%m-%d %H:%M:%S.%f')
            scan_count += 1
            # 合并一分钟监测车数据为一帧
            if time_current.tm_min == time_min:
                fp_data_total += i[1]
                # # 计算电平值出现次数
                # for j in i[1]:
                #     if j/10 not in amp_dict.keys():
                #         amp_dict.update({j/10: 1})
                #     else:
                #         amp_dict[j/10] += 1

                cvg_data += ocy(i[1], i[0][2], i[0][3], i[0][4])
            else:
                # fp_data_min = (fp_data_total / scan_count).round()
                time_str = i[0][10]
                longitude = i[0][-4]
                latitude = i[0][-3]
                # col, row = coordinate(longitude, latitude)
                index = freq_band_index_split(int(i[0][2]), int(i[0][3]), int(i[0][4]))
                t = dict(zip(index / 1000000, cvg_data))
                freqregion = str(int(self.start_freq / 1000000)) + '-' + str(int(self.stop_freq / 1000000))
                activepoint = freq_band_split(t, int(freqregion.split('-')[0]), int(freqregion.split('-')[1]))
                # cvg = round(sum(cvg_data)/(scan_count*first_frame[0][-1])*100, 2)
                # ocy_data = ocy(fp_data_min, i[0][2], i[0][3], i[0][4])
                # ocy_res = round(sum(ocy_data)/ocy_data.size * 100, 2)
                with open('/storage/sdb/data/car', 'a') as f:
                    f.write(mfid+'|'+str(longitude)+'|'+str(latitude)+'|'+time_str+'|'+str(scan_count)+'|'+str(activepoint)+'|'+str(amp_dict))
                    f.write('\n')
                # # 将每帧数据经纬度与台站数据库比对
                # for j in station:
                #     if haversine(longitude/100000000, latitude/100000000, j[0], j[1]) < j[2]:
                #         cvg = round(sum(cvg_data)/(scan_count*first_frame[0][-1])*100, 2)
                #         ocy_data = ocy(fp_data_min, i[0][2], i[0][3], i[0][4])
                #         ocy_res = round(sum(ocy_data)/ocy_data.size * 100, 2)
                #         with open('car', 'a') as f:
                #             f.write(str(col)+','+str(row)+','+j[3]+','+j[4]+','+str(ocy_res)+','+str(cvg))
                #             f.write('\n')

                amp_dict = {}
                fp_data_total = np.zeros(first_frame[0][-1])
                cvg_data = np.zeros(first_frame[0][-1])
                time_min = time_current.tm_min
                scan_count = 0

        return 1

    def calc(self):
        """
        判断文件为监测车或固定站并调用对应方法
        """
        try:
            if self.flag == 'F':
                self.monitoring_facility_data_file()
            else:
                self.monitoring_car_data_min()

        except Exception as e:
            print(e)
            with open('error', 'a') as f:
                f.write(str(e)+'-----'+self.filepath)
                f.write('\n')
        # if self.flag == 'F':
        #     self.monitoring_facility_data_file()
        # else:
        #     self.monitoring_car_data_min()
        #     print(self.file.name)


def freq_band_index_split(start_freq, stop_freq, step):
    """
    通过开始频率和结束频率以及步进返回拆分后列表
    单位统一为   hz
    """
    if (stop_freq-start_freq) % step == 0:
        res = [i for i in range(start_freq, stop_freq, step)]
        res.append(stop_freq)
    else:
        raise ValueError

    return np.array(res)


def freq_band_split(d, start_freq, stop_freq):
    """
    返回指定范围内的字典元素
    """
    res = {key: value for key, value in d.items() if start_freq <= key <= stop_freq}

    return res


def ocy(fp_data, start_freq, stop_freq, step):
    """
    计算每一帧频点是否超过门限，返回bool数组
    单位  hz
    """
    fp_data = list(np.array(fp_data)/10)
    so = CInvoker(fp_data, start_freq / 1000, stop_freq / 1000, step / 1000)
    auto = so.auto_threshold()
    np_fp_data = np.array(fp_data)
    np_auto = np.array(auto)

    return np_fp_data > np_auto


def remove_null_from_dict(d):
    """
    去掉字典中超过门限次数为0的元素
    """
    res = d.copy()
    for item in d.items():
        if item[1] == 0:
            res.pop(item[0])

    return res


# def match_stationid(longitude, latitude):
#     """
#     通过经纬度匹配频段区域，台站id
#     """
#     cursor = hc.get_hive_cursor('172.18.140.8', 'spectrum_evaluation')
#     sql = "select stat_lg,stat_la,st_serv_r,freqregion,staid from station"
#     res = hc.execute_sql(cursor, sql)
#     for i in res:
#         if haversine(longitude, latitude, i[0], i[1]) < i[2]:
#             yield i[3], i[4]


def coordinate(longitude, latitude):
    """
    返回地图网格row,column
    """
    cell_size = 0.01
    lb_coord_x = 72
    lb_coord_y = 3

    col = int((longitude/100000000 - lb_coord_x)/cell_size)
    row = int((latitude/100000000 - lb_coord_y)/cell_size)

    return col, row


def haversine(lon1, lat1, lon2, lat2):
    """
    传入两点经纬度，计算两点之间球体距离
    返回单位 m
    """
    # 将十进制度数转化为弧度
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # Haversine公式
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371.137  # 地球平均半径，单位为公里
    d = c * r * 1000

    return round(d, 3)


if __name__ == '__main__':

    files = traverse_file.get_all_file('/storage/sdb/data/', 'bin')
    count = 0
    for i in files:
        name = os.path.basename(i)
        if len(name.split('_')) == 9:
            count += 1
            print('resolving file: {0}  ----  {1}'.format(name, count))
            SpectrumStatistics(i).calc()
