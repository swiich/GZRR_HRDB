import xml.etree.cElementTree as et
from hbase import hbase_connector
from time import *
from os.path import *

file_name = '11000001111111-B_PScan(VHF)-838a7074-ff73-49c3-a65d-86dd0ec967dd-20180801152800.0115.des'
file = hbase_connector.Hbase('172.39.8.61').table_row('desc', 'fsc.%s' % file_name)[b'cf1:data']
with open(file_name, 'wb') as f:
    f.write(file)
root = et.parse(file_name)

equipment = root.find('equipment')
equid = equipment.find('equid').text
task = equipment.find('task')
dataguid = task.find('dataguid').text
taskid = task.find('taskid').text
start_time = strftime('%Y-%m-%d %H:%M:%S', (strptime(task.find('starttime').text, "%Y/%m/%d %H:%M:%S")))
stop_time = strftime('%Y-%m-%d %H:%M:%S', (strptime(task.find('stoptime').text, "%Y/%m/%d %H:%M:%S")))
# stop_time = int(mktime(strptime(task.find('stoptime').text, "%Y/%m/%d %H:%M:%S")))
file_location = '/spectrum_data/%s.FSCAN' % splitext(basename(file_name))[0]
print(file_location)
# result = (dataguid, taskid, equid, start_time, stop_time, file_location)
#
# with open('desc.txt', 'a+', newline='') as f:
#     for i, e in enumerate(result):
#         if not i == len(result)-1:
#             f.write(str(e)+',')
#         else:
#             f.write(str(e))
#     f.write('\n')
