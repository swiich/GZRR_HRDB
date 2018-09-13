from tools.analyse_stream import Read
import os
import numpy as np
import xml.etree.cElementTree as Et
from time import strftime, strptime


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
    """

    获取文件监测开始时间，结束时间，监测数据类型，文件大小，mfid

    """
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


def file_index(file, file_des):
    """

    生成文件索引表内容, 地区返回内容表， 任务返回内容表

    """
    des_result = xml_parser(file_des)
    file_result = get_file_info(file)


def xml_parser(xml_file):
    """

    解析描述文件

    """
    xml_root = Et.parse(xml_file)

    areacode = xml_root.find('areacode').text
    mfname = xml_root.find('mfname').text
    equipment = xml_root.find('equipment')
    equid = equipment.find('equid').text
    equiname = equipment.find('equname').text
    task = equipment.find('task')
    dataguid = task.find('dataguid').text
    taskid = task.find('taskid').text
    t_start_time = strftime('%Y-%m-%d %H:%M:%S', (strptime(task.find('starttime').text, "%Y/%m/%d %H:%M:%S")))
    t_stop_time = strftime('%Y-%m-%d %H:%M:%S', (strptime(task.find('stoptime').text, "%Y/%m/%d %H:%M:%S")))

    result = (dataguid, taskid, equid, t_start_time, t_stop_time)
    return result

