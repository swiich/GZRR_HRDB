from tools.analyse_stream import Read
import os
import numpy as np
import xml.etree.cElementTree as Et
import time
import datetime
import hive_connector as hc
from tools.signal_handler import get_businessid
from tools import webservice as ws


class MBasicDataTable(Read):
    # def __init__(self, file_name):
    #     super(MBasicDataTable, self).__init__(file_name)
        # self.file_name = file_name

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

    获取文件监测开始时间，结束时间，监测数据类型，文件大小，文件每分钟数据大小

    """
    inf = next(MBasicDataTable(file).header_payload())
    m_start_time = inf[0]
    m_stop_time = ''
    m_data_type = inf[1]
    file_size = round(os.path.getsize(file)/1024**2, 2)
    for f in MBasicDataTable(file).header_payload():
        m_stop_time = f[0]

    m_start = time.mktime(time.strptime(m_start_time, '%Y-%m-%d %H:%M:%S.%f'))
    m_stop = time.mktime(time.strptime(m_stop_time, '%Y-%m-%d %H:%M:%S.%f'))
    file_size_min = round(file_size/(m_stop-m_start)*60, 2)

    result = (m_start_time, m_stop_time, m_data_type, file_size, file_size_min)
    return result


def file_index(file, file_des, return_type):
    """

    生成文件索引表内容, 地区返回内容表， 任务返回内容表插入数据库表中

    """
    des_result = xml_parser(file_des, return_type)
    file_result = get_file_info(file)

    if return_type == 'file_index':
        result = (des_result[0], des_result[1], des_result[6], des_result[2], des_result[5], file_result[0],
                  file_result[1], des_result[3], des_result[4], file_result[3],
                  '/data/ftp/'+os.path.basename(file), file_result[2])
        sql = "insert into table file_index values ('{0}','{1}','{2}','{3}','{4}'," \
              "'{5}','{6}','{7}','{8}','{9}','{10}','{11}')".format(*result)
    elif return_type == 'device_info':
        result = (des_result[0], des_result[5], des_result[1], des_result[2], des_result[3], des_result[4])
        sql = "insert into table deviceinfo values ('{0}','{1}','{2}','{3}','{4}','{5}')".format(*result)
        sql_filter = 'select * from deviceinfo where mfid="{0}" and equid="{1}"'.format(des_result[5], des_result[2])
    elif return_type == 'task_info':
        result = (des_result[0], des_result[1], des_result[9], des_result[2], des_result[3],
                  des_result[4], des_result[5], des_result[6], des_result[7],  des_result[8], des_result[9])
        if des_result[9] == 'finished':
            sql = "insert into table taskinfo partition (status=1) values ('{0}','{1}','{2}','{3}','{4}'," \
                  "'{5}','{6}','{7}','{8}','{9}', '{10}')".format(*result)
        else:
            result = list(result)
            result[5] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            sql = "insert into table taskinfo partition (status=0) values ('{0}','{1}','{2}','{3}','{4}'," \
                  "'{5}','{6}','{7}','{8}','{9}', '{10}')".format(*result)

        sql_filter = 'select * from taskinfo where taskid="{0}" and equid="{1}"'.format(des_result[0], des_result[2])

    cursor = hc.get_hive_cursor('172.39.8.60', 'db_data_store')
    # 去重
    if return_type == 'device_info' or return_type == 'task_info':
        res = hc.execute_sql(cursor, sql_filter)
        if not res:
            hc.execute_sql_insert(cursor, sql)
        else:
            return 0
    else:
        hc.execute_sql_insert(cursor, sql)

    return 1


def xml_parser(xml_file, return_type):
    """

    解析描述文件

    """
    xml_root = Et.parse(xml_file).find('result')

    equid = xml_root.find('equid').text
    mfid = xml_root.find('mfid').text
    areacode = mfid[0:6]
    mfname, equname, tmp, tmp = ws.query_device(mfid, equid)
    dataguid = os.path.basename(xml_file).split('.')[0]
    taskid = xml_root.find('taskid').text
    feature = xml_root.find('feature').text
    userid = xml_root.find('userid').text
    appid = xml_root.find('appid').text
    res_list = xml_root.find('list').find('segment')
    start_freq = float(res_list.find('startfreq').text)
    stop_freq = float(res_list.find('stopfreq').text)
    bid_tmp, flag = get_businessid(start_freq/1000000, stop_freq/1000000)
    businessid = bid_tmp[0][0] if isinstance(bid_tmp, list) else bid_tmp
    if not flag:
        cursor = hc.get_hive_cursor('172.18.140.8', 'rmdsd')
        sql = 'insert into table rmbt_service_freqdetail ' \
              'values ("{0}","00000000-0000-0000-0000-000000000000","{1}-{2}Mhz",{1},{2},25.0)'.format(
               businessid, start_freq/1000000, stop_freq/1000000)
        hc.execute_sql_insert(cursor, sql)
    paramxml_str, t_start_time, t_stop_time, status = ws.query_tasks(taskid)

    return_dict = {
        'file_index': (dataguid, taskid, equid, t_start_time, t_stop_time, businessid, mfid),
        'task_info': (taskid, feature, equid, t_start_time, t_stop_time, userid, paramxml_str, appid, dataguid, mfid, status),
        'device_info': (areacode, mfname, equid, equname, feature, mfid),
        'b_info': (mfid, start_freq, stop_freq)
    }
    result = return_dict[return_type]

    return result


def des_save(xml_file):
    """

    将描述文件内容存入hive表

    """
    # try:

    xml_root = Et.parse(xml_file).find('result')
    taskid = xml_root.find('taskid').text
    mfid = xml_root.find('mfid').text
    areacode = mfid[0:6]
    equid = xml_root.find('equid').text
    mfname, equname, equmodel, feature_list = ws.query_device(mfid, equid)
    feature = xml_root.find('feature').text
    paramxml_str, t_start_time, t_stop_time, status = ws.query_tasks(taskid)
    appid = xml_root.find('appid').text
    userid = xml_root.find('userid').text
    dataguid = os.path.basename(xml_file).split('.')[0]

    res = '<?xml version="1.0" encoding="utf-8"?>' \
          '<datamapping>' \
          '<mfid>{0}</mfid>' \
          '<mfname>{1}</mfname>' \
          '<areacode>{2}</areacode>' \
          '<equipment>' \
          '<equid>{3}</equid>' \
          '<equname>{4}</equname>' \
          '<equmodel>{5}</equmodel>' \
          '{6}' \
          '<task>' \
          '<taskid>{7}</taskid>' \
          '<taskname></taskname>' \
          '<tasktype></tasktype>' \
          '<starttime>{8}</starttime>' \
          '<stoptime>{9}</stoptime>' \
          '<appid>{10}</appid>' \
          '<feature>{11}</feature>' \
          '<userid>{12}</userid>' \
          '<participateuser></participateuser>' \
          '<taskarea>{13}</taskarea>' \
          '<dataguid>{14}</dataguid>' \
          '<paramxml>{15}</paramxml>' \
          '</task>' \
          '</equipment>' \
          '</datamapping>'.format(mfid, mfname, areacode, equid, equname, equmodel, feature_list,
                                  taskid, t_start_time, t_stop_time, appid, feature, userid, areacode,
                                  dataguid, paramxml_str)

    cursor = hc.get_hive_cursor('172.39.8.60', 'db_data_store')
    sql = "insert into table des_file values ('{0}','{1}')".format(os.path.basename(xml_file), res)
    hc.execute_sql_insert(cursor, sql)
    return True
    # except Exception as e:
    #     print("error at des_save ", e)
    #     return False


def headtail_time(file):
    """

    获取文件的首位时间

    """
    start_time = next(MBasicDataTable(file).header_payload())[0]
    for frame in MBasicDataTable(file).header_payload():
        end_time = frame[0]

    return start_time, end_time


def update_tasktime():
    """

    更新波尔接口中status不为finished的taskinfo

    """
    cursor = hc.get_hive_cursor('172.39.8.60', 'db_data_store')
    sql = 'select * from taskinfo where status=0'
    task_list = hc.execute_sql(cursor, sql)
    hc.execute_sql_insert(cursor, 'alter table taskinfo drop partition (status=0)')
    for task in task_list:
        paramxml, start_time, stop_time, status = ws.query_tasks(task[0])
        if status == 'finished':
            task = list(task)
            task[4] = start_time
            task[5] = stop_time
            sql = "insert into table taskinfo partition (status=1) values ('{0}','{1}','{2}','{3}','{4}'," \
                  "'{5}','{6}','{7}','{8}','{9}') ".format(*task)
            hc.execute_sql_insert(cursor, sql)
        else:
            sql = "insert into table taskinfo partition (status=0) values ('{0}','{1}','{2}','{3}','{4}'," \
                  "'{5}','{6}','{7}','{8}','{9}') ".format(*task)
            hc.execute_sql_insert(cursor, sql)
