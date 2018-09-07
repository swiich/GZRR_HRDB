import hive_connector as hc


def occupancy_frequencband(s_type, s_time):
    cursor = hc.get_hive_cursor('172.39.8.62', 'db_data_store')
    if s_type == 'min':
        sql = 'select * from amp_info where statstype=4 and to_date(statstime)="{0}" and hour(statstime)="{1}" and minute(statstime)="{2}"'.format(*s_time)
    elif s_type == 'hour':
        sql = 'select * from amp_info where statstype=4 and to_date(statstime)="{0}" and hour(statstime)="{1}"'.format(*s_time)
    elif s_type == 'day':
        sql = 'select * from amp_info where statstype=4 and to_date(statstime)="{0}"'.format(*s_time)
    elif s_type == 'year':
        sql = 'select * from amp_info where statstype=4 and year(statstime)="{0}"'.format(*s_time)
    else:
        raise ValueError

    res = hc.execute_sql(cursor, sql)

    if res:
        scan_count = 0
        occ_count = 0
        for i in res:
            scan_count_tmp = 0
            occ_count_tmp = 0
            amp_dict = eval(i[3])

            for v in amp_dict.values():
                scan_count_tmp += v

            occ_count_tmp += i[4]

            scan_count += scan_count_tmp
            occ_count += occ_count_tmp

        return round(occ_count/scan_count, 2)
    else:
        return None


def get_bt_id(freq_band, dbname):
    cursor = hc.get_hive_cursor('172.39.8.62', 'rmdsd')
    sql = ''
