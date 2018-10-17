from suds.client import Client
import xml.etree.cElementTree as Et

url = 'http://172.18.130.16:8760/IIRMCompoundService'
client = Client(url)


def query_tasks(taskid):
    result_str = client.service.M_QueryTasks('<query><get><taskid>%s</taskid></get></query>' % taskid)

    xml_root = Et.fromstring(result_str)
    result = xml_root.find('result')
    tasks = result.find('tasks')
    task = tasks.find('task')
    paramxml = task.find('paramxml')

    paramxml_str = Et.tostring(paramxml, 'utf-8').decode().\
        replace('\n', '').replace('\t', '').replace('<paramxml>', '').replace(' ', '').replace('</paramxml>', '')

    return paramxml_str


def query_device(mfid, equid):
    result_str = client.service.M_QueryDeviceInfo(
        '<query><get><mfid>{0}</mfid><equid>{1}</equid></get></query>'.format(mfid, equid))

    xml_root = Et.fromstring(result_str)
    result = xml_root.find('result')
    mfname = result.find('mfname').text
    equname = result.find('equname').text

    return (mfname, equname)
