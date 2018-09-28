from tools.analyse_stream import Read
import os
import numpy as np
import xml.etree.cElementTree as Et
from time import strftime, strptime
import hive_connector as hc
from tools.signal_handler import get_businessid
# import sys
# sys.path.append(r'自己包的路径')


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
    file_size = round(os.path.getsize(file)/1024**2, 2)
    mfid = os.path.basename(file).split('-')[0]
    for f in MBasicDataTable(file).header_payload():
        m_stop_time = f[0]

    result = (m_start_time, m_stop_time, m_data_type, file_size, mfid)
    return result


def file_index(file, file_des, return_type):
    """

    生成文件索引表内容, 地区返回内容表， 任务返回内容表插入数据库表中

    """
    des_result = xml_parser(file_des, return_type)
    file_result = get_file_info(file)

    if return_type == 'file_index':
        result = (des_result[0], des_result[1], file_result[4], des_result[2], des_result[5], file_result[0],
                  file_result[1], des_result[3], des_result[4], file_result[3],
                  '/data/fscan&spectrum/'+os.path.basename(file), file_result[2])
        sql = "insert into table file_index values ('{0}','{1}','{2}','{3}','{4}'," \
              "'{5}','{6}','{7}','{8}','{9}','{10}','{11}')".format(*result)
    elif return_type == 'device_info':
        result = (des_result[0], file_result[4], des_result[1], des_result[2], des_result[3], des_result[4])
        sql = "insert into table deviceinfo values ('{0}','{1}','{2}','{3}','{4}','{5}')".format(*result)
    elif return_type == 'task_info':
        result = (des_result[0], des_result[1], file_result[4], des_result[2], des_result[3],
                  des_result[4], des_result[5], des_result[6], des_result[7],  des_result[8])
        sql = "insert into table taskinfo values ('{0}','{1}','{2}','{3}','{4}'," \
              "'{5}','{6}','{7}','{8}','{9}')".format(*result)

    cursor = hc.get_hive_cursor('172.39.8.60', 'db_data_store')
    hc.execute_sql_insert(cursor, sql)

    return result


def xml_parser(xml_file, return_type):
    """

    解析描述文件

    """
    xml_root = Et.parse(xml_file)

    areacode = xml_root.find('areacode').text
    mfname = xml_root.find('mfname').text
    equipment = xml_root.find('equipment')
    equid = equipment.find('equid').text
    equname = equipment.find('equname').text
    task = equipment.find('task')
    dataguid = task.find('dataguid').text
    taskid = task.find('taskid').text
    t_start_time = strftime('%Y-%m-%d %H:%M:%S', (strptime(task.find('starttime').text, "%Y/%m/%d %H:%M:%S")))
    t_stop_time = strftime('%Y-%m-%d %H:%M:%S', (strptime(task.find('stoptime').text, "%Y/%m/%d %H:%M:%S")))
    feature = task.find('feature').text
    userid = task.find('userid').text
    appid = task.find('appid').text
    paramxml = task.find('paramxml')
    start_freq = float(paramxml.find('startfreq').text)
    stop_freq = float(paramxml.find('stopfreq').text)
    bid_tmp = get_businessid(start_freq/1000000, stop_freq/1000000)
    businessid = bid_tmp[0][0] if isinstance(bid_tmp, list) else bid_tmp

    paramxml_str = Et.tostring(paramxml, 'utf-8').decode().replace('\n', '')

    if return_type == 'file_index':
        result = (dataguid, taskid, equid, t_start_time, t_stop_time, businessid)
    elif return_type == 'task_info':
        result = (taskid, feature, equid, t_start_time, t_stop_time, userid, paramxml_str, appid, dataguid)
    elif return_type == 'device_info':
        result = (areacode, mfname, equid, equname, feature)
    else:
        raise ValueError

    return result


def des_save(xml_file):
    """

    将描述文件内容存入hive表

    """
    try:
        res = open(xml_file, encoding='utf-8').read().replace('\n', '')
        cursor = hc.get_hive_cursor('172.39.8.60', 'db_data_store')
        sql = "insert into table des_file values ('{0}','{1}')".format(os.path.basename(xml_file), res)
        hc.execute_sql_insert(cursor, sql)
        return True
    except:
        return False
