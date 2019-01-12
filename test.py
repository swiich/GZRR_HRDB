# standard = {
#         'b_sglfreqmeas': 'B_SglFreqMeas',
#         'b_wbfftmon': 'B_WBFFTMon',
#         'b_fscan': 'B_FScan',
#         'b_pscan': 'B_PScan',
#         'b_mscan': 'B_MScan',
#         'b_sglfreqdf': 'B_SglFreqDF',
#         'b_wbdf': 'B_WBDF',
#         'b_fscandf': 'B_FScanDF',
#         'b_mscandf': 'B_MScanDF',
#         'b_digsglrecdecode': 'B_DigSglRecDecode',
#         'b_anatvdem': 'B_AnaTVDem',
#         'b_digtvdem': 'B_DigTVDem',
#         'b_digbroadcastdem': 'B_DigBroadcastDem',
#         'b_speccommsyssgldem': 'B_SpecCommSysSglDem',
#         'b_occumeas': 'B_OccuMeas',
#         'b_ifsglinte': 'B_IFSglInte',
#         'b_wbsglinte': ' B_WBSglInte',
#         'b_fscansglinte': 'B_FScanSglInte',
#         'b_linkantedev': 'B_LinkAnteDev',
#         'b_selftest': 'B_SelfTest',
#         'b_queryfacidevstatus': 'B_QueryFaciDevStatus',
#         'b_setdevicepower': 'B_SetDevicePower',
#         'b_querydeviceinfo': 'B_QueryDeviceInfo',
#         'b_stopmeas': 'B_StopMeas',
#         'digsglrecdecode': 'B_DigSglRecDecode', 'anatvdem': 'B_AnaTVDem', 'digtvdem': 'B_DigTVDem',
#         'digbroadcastdem': 'B_DigBroadcastDem', 'speccommsyssgldem': 'B_SpecCommSysSglDem', 'occumeas': 'B_OccuMeas',
#         'ifsglinte': 'B_IFSglInte', 'wbsglinte': 'B_WBSglInte', 'fscansglinte': 'B_FScanSglInte',
#         'linkantedev': 'B_LinkAnteDev', 'selftest': 'B_SelfTest', 'queryfacidevstatus': 'B_QueryFaciDevStatus',
#         'setdevicepower': 'B_SetDevicePower', 'querydeviceinfo': 'B_QueryDeviceInfo', 'stopmeas': 'B_StopMeas'
# }
# for item in standard.items():
#         print(item)

# import re
#
# source = '<s> </s><s>   </s> <s>  </s><s>  </s>'
# p1 = '> +<'
# pattern = re.compile(p1)
# print(re.sub(pattern, '><', source))

# a = datetime.datetime(2018, 1, 20, 11, 14, 45, 545000)
# b = datetime.datetime(2018, 1, 20, 12, 14, 45, 545000)
# print((b-a).seconds)

# timestr = '2018-10-10 13:51:30.545000'
# date = datetime.datetime.strptime(timestr, '%Y-%m-%d %H:%M:%S.%f')
# print(date)
import numpy as np
from tools.analyse_stream import Read
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
a = next(MBasicDataTable('52010000120021_f3020516-27c3-4654-b098-9c67c92e90a1_b1925d45-36bb-42a2-984a-3db50c6cbe9c_20190104153324.bin').header_payload())
print(a)
