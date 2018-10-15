# coding=utf-8


# 代码有任何变动需要同时修改 /usr/local/python3/lib/python3.6/hbase_connector.py
import happybase


class Hbase:
    def __init__(self, ip):
        self.conn = happybase.Connection(ip)

    def get_connection(self):
        return self.conn

    def show_tables(self):
        return self.conn.tables()

    def get_table(self, t_name):
        return self.conn.table(t_name)

    def table_row(self, t_name, row):
        tb = self.conn.table(t_name)
        res = tb.row(row)
        return res

    def delete_table(self, t_name):
        if self.conn.is_table_enabled(t_name):
            self.conn.disable_table(t_name)

        return self.conn.delete_table(t_name)

    def __del__(self):
        self.conn.close()


# if __name__ == '__main__':
#     conn = Hbase('172.39.8.62')
#     print(conn)
    # connection = conn.get_connection()
    # connection.create_table(
    #     'desc',
    #     {
    #         'cf1:': dict()
    #     }
    # )
    # table = conn.get_table('desc')
    # print(conn.table_row('desc', '11000001111111-B_PScan(VHF)-838a7074-ff73-49c3-a65d-86dd0ec967dd-20180801152800.0115.des'))

    # file = traverse_file.get_all_file('../data', 'des')
    # for i in file:
    #     file_name = os.path.basename(i)
    #     with open(i) as f:
    #         data = {
    #             'cf1:data': f.read()
    #         }
    #     with table.batch() as batch:
    #         batch.put(row='fsc.%s'%file_name, data=data)
    #         print(i)
    # table = conn.get_table('spectrum_data')
    # for k, v in table.scan(row_prefix=b'fsc.spectrum21678'):
    #     print(k)
    #     break
    # file = traverse_file.get_all_file('../data/frame', 'csv')
    # for i, e in enumerate(file):
    #     file_name = os.path.basename(e)
    #     with open(e, 'rb') as f:
    #         data = {
    #             'cf1:data': f.read()
    #         }
    #
    #     with table.batch() as batch:
    #         batch.put(row='fsc.%s'%file_name, data=data)
    #     print(i)

