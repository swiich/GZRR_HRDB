# coding=utf-8

import xml.etree.cElementTree as Et
from tools.signal_handler import *
from tools.file_info import file_index, des_save, xml_parser, get_file_info
from tools.MyThread import MyThread
from tools.traverse_file import get_all_file
import os
import time
import sys
import shutil

standard = {
    'b_sglfreqmeas': 'B_SglFreqMeas', 'b_wbfftmon': 'B_WBFFTMon',
    'b_fscan': 'B_FScan', 'b_pscan': 'B_PScan', 'b_mscan': 'B_MScan', 'b_sglfreqdf': 'B_SglFreqDF',
    'b_wbdf': 'B_WBDF', 'b_fscandf': 'B_FScanDF', 'b_mscandf': 'B_MScanDF',
    'sglfreqmeas': 'B_SglFreqMeas', 'wbfftmon': 'B_WBFFTMon', 'b_digsglrecdecode': 'B_DigSglRecDecode',
    'b_anatvdem': 'B_AnaTVDem', 'b_digtvdem': 'B_DigTVDem', 'b_digbroadcastdem': 'B_DigBroadcastDem',
    'b_speccommsyssgldem': 'B_SpecCommSysSglDem', 'b_occumeas': 'B_OccuMeas',
    'b_ifsglinte': 'B_IFSglInte', 'b_wbsglinte': ' B_WBSglInte', 'b_fscansglinte': 'B_FScanSglInte',
    'b_linkantedev': 'B_LinkAnteDev', 'b_selftest': 'B_SelfTest', 'b_queryfacidevstatus': 'B_QueryFaciDevStatus',
    'b_setdevicepower': 'B_SetDevicePower', 'b_querydeviceinfo': 'B_QueryDeviceInfo', 'b_stopmeas': 'B_StopMeas',
    'digsglrecdecode': 'B_DigSglRecDecode', 'anatvdem': 'B_AnaTVDem', 'digtvdem': 'B_DigTVDem',
    'digbroadcastdem': 'B_DigBroadcastDem', 'speccommsyssgldem': 'B_SpecCommSysSglDem', 'occumeas': 'B_OccuMeas',
    'ifsglinte': 'B_IFSglInte', 'wbsglinte': 'B_WBSglInte', 'fscansglinte': 'B_FScanSglInte',
    'linkantedev': 'B_LinkAnteDev', 'selftest': 'B_SelfTest', 'queryfacidevstatus': 'B_QueryFaciDevStatus',
    'setdevicepower': 'B_SetDevicePower', 'querydeviceinfo': 'B_QueryDeviceInfo', 'stopmeas': 'B_StopMeas',
    'fscan': 'B_FScan', 'pscan': 'B_PScan', 'mscan': 'B_MScan', 'sglfreqdf': 'B_SglFreqDF', 'wbdf': 'B_WBDF',
    'fscandf': 'B_FScanDF', 'mscandf': 'B_MScanDF',
}


def feature_modify(xml_file):
    """
    将描述文件中feature按照原子服务2.0规范修改
    """
    root = Et.parse(xml_file)
    feature = root.getroot().find('result').find('feature')

    if feature.text.lower() in standard.keys():
        feature.text = standard[feature.text.lower()]
        root.write(xml_file, encoding='utf-8')

        return standard[feature.text.lower()]
    else:
        return False


class WatchDog():
    """ 监控path文件夹文件 while循环读取文件夹文件变动取消watchdog监控模块"""
    def __init__(self, path):
        self.path = path

    def on_created(self):
        file_bin_list = get_all_file(self.path, 'bin')
        file_xml_list = get_all_file(self.path, 'xml')
        if not file_bin_list:
            sys.stdout.write('\rNo task to be processed now, directory is empty... every 10 secs to get tasks...')
            sys.stdout.flush()
        else:
            print('processing... task count: {0}'.format(len(file_bin_list)))
            try:

                # 遍历描述文件，并找出对应的bin文件
                for file_xml in file_xml_list:
                    if str(file_xml).split('.')[0]+'.bin' in file_bin_list:
                        file_bin = str(file_xml.split('.')[0])+'.bin'

                        # 判断文件是否传输完毕
                        while True:
                            oldl = os.path.getsize(file_bin)
                            time.sleep(1)
                            currentl = os.path.getsize(file_bin)
                            if currentl == oldl:
                                break

                        # 移除小于20M的监测文件
                        if os.path.getsize(file_bin) < 20971520:
                            os.remove(file_xml)
                            os.remove(file_bin)
                            print('file removed...')
                            continue

                        # 将描述文件中feature按照原子服务2.0规范修改
                        feature_modify(file_xml)
                        print(file_xml.split('.')[0])

                        xml_info = xml_parser(file_xml, 'b_info')
                        mfid = xml_info[0]
                        start_freq = xml_info[1] / 1000000
                        stop_freq = xml_info[2] / 1000000
                        file_info = get_file_info(file_bin)
                        file_size_min = file_info[-1]
                        data_type = file_info[2]

                        file_resolve(file_bin, mfid, start_freq, stop_freq, file_size_min, data_type)
                        t2 = MyThread(file_index, (file_bin, file_xml, 'file_index'))
                        t3 = MyThread(file_index, (file_bin, file_xml, 'task_info'))
                        t4 = MyThread(file_index, (file_bin, file_xml, 'device_info'))
                        t5 = MyThread(des_save, (file_xml,))

                        t2.start()
                        t3.start()
                        t4.start()
                        t5.start()

                        res = [t2, t3, t4, t5]
                        for t in res:
                            t.join()
                        os.remove(file_bin)
                        os.remove(file_xml)

                        # 记录正常处理后文件
                        with open('/home/fileresolve_log/correct.log', 'a') as f:
                            f.write(os.path.basename(file_xml) + '--' + os.path.basename(file_bin) + str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))) +'\n')

            except Exception as e:
                # 异常日志
                currenttime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                try:
                    with open('/home/fileresolve_log/error.log', 'a') as f:
                        f.write(os.path.basename(file_bin) + '--' + str(e) + '--' + str(currenttime) + '\n')

                    # 将错误文件移动到日志目录
                    shutil.move(file_bin, '/home/fileresolve_log/error_file/')
                    shutil.move(file_xml, '/home/fileresolve_log/error_file/')

                    print('error at: ' + os.path.basename(file_bin))
                    print('log path: /home/fileresolve_log/')

                except Exception as e:
                    with open('/home/fileresolve_log/error.log', 'a') as f:
                        f.write(str(e) + '--' + str(currenttime) + '\n')
                    os.remove(file_bin)
                    os.remove(file_xml)
                    print('error happenes before getting tasks ------ log path: /home/fileresolve_log/')

                except shutil.Error as e:
                    with open('/home/fileresolve_log/error.log', 'a') as f:
                        f.write(str(e) + '--' + str(currenttime) + '\n')
                    os.remove(file_bin)
                    os.remove(file_xml)
                    print('move error file failed, delete')


if __name__ == '__main__':

    try:
        while True:
            WatchDog('/home/ftp').on_created()

            time.sleep(10)

    except KeyboardInterrupt:
        print('User Interrupts')
