from suds.client import Client
import xml.etree.cElementTree as Et
from time import strftime, strptime

url = 'http://172.18.130.16:8760/IIRMCompoundService'
client = Client(url)


def query_tasks(taskid):
    result_str = client.service.M_QueryTasks('<query><get><taskid>%s</taskid></get></query>' % taskid)

    xml_root = Et.fromstring(result_str)
    result = xml_root.find('result')
    tasks = result.find('tasks')
    task = tasks.find('task')
    paramxml = task.find('paramxml')

    t_start_time = strftime('%Y-%m-%d %H:%M:%S', (strptime(task.find('starttime').text, "%Y%m%d%H%M%S")))
    t_stop_time = strftime('%Y-%m-%d %H:%M:%S', (strptime(task.find('stoptime').text, "%Y%m%d%H%M%S")))

    paramxml_str = Et.tostring(paramxml, 'utf-8').decode().\
        replace('\n', '').replace('\t', '').replace('<paramxml>', '').replace(' ', '').replace('</paramxml>', '')

    return paramxml_str, t_start_time, t_stop_time


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

    return mfname, equname, equmodel, feature_input

# print(query_tasks('000007BB-20AB-2D70-69FE-7FF6109831A7'))