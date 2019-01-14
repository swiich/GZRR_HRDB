from suds.client import Client
import xml.etree.cElementTree as Et
from time import strftime, strptime

url = 'http://172.18.130.16:8760/IIRMCompoundService'
client = Client(url)

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


def query_tasks(taskid):
    result_str = client.service.M_QueryTasks('<query><get><taskid>%s</taskid></get></query>' % taskid)

    xml_root = Et.fromstring(result_str)
    result = xml_root.find('result')
    tasks = result.find('tasks')
    task = tasks.find('task')
    paramxml = task.find('paramxml')

    t_start_time = strftime('%Y-%m-%d %H:%M:%S', (strptime(task.find('starttime').text, "%Y%m%d%H%M%S")))
    t_stop_time = strftime('%Y-%m-%d %H:%M:%S', (strptime(task.find('stoptime').text, "%Y%m%d%H%M%S")))
    status = task.find('status').text

    paramxml_str = Et.tostring(paramxml, 'utf-8').decode().\
        replace('\n', '').replace('\t', '').replace('<paramxml>', '').replace(' ', '').replace('</paramxml>', '')

    return paramxml_str, t_start_time, t_stop_time, status


def query_device(mfid, equid):
    result_str = client.service.M_QueryDeviceInfo(
        '<query><get><mfid>{0}</mfid><equid>{1}</equid></get></query>'.format(mfid, equid))

    xml_root = Et.fromstring(result_str)
    result = xml_root.find('result')
    mfname = result.find('mfname').text
    equname = result.find('equname').text
    equmodel = result.find('equmodel').text
    feature_input = Et.tostring(result.find('featurelist').findall('feature')[-1], encoding='utf-8').\
        decode().replace('\n', '').replace(' ', '')

    # 波尔接口中查询出的code和feature转换为原子服务2.0规范
    feature_tmp = Et.fromstring(feature_input)
    if feature_tmp.find('code').text.lower() in standard.keys():
        feature_tmp.find('code').text = standard[feature_tmp.find('code').text.lower()]
    feature_list = Et.tostring(feature_tmp, encoding='utf-8').decode()

    return mfname, equname, equmodel, feature_list
