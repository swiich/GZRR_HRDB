from tools.analyse_stream import Read
import struct
from result_table_scripts.m_data_base import get_file_info

file = './data/11000001111111-B_PScan(VHF)-838a7074-ff73-49c3-a65d-86dd0ec967dd-20180801152800.0115.FSCAN'

# for a in Read(file, 'spm').header_payload():
#     print(a)
a = get_file_info(file)
print(a)

