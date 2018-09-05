import hdfs
import os

# upload()函数中Progress-回调函数跟踪进度
# length指定读取字节数长度
# chunk_size指定每次读取字节数，返回generator，使文件内容变为流数据


def download_file(hdfs_location, local):
    """
    :param hdfs_location: hdfs路径+文件名
    :param local: 本地路径+文件名
    :return:
    """
    client = hdfs.Client('http://172.39.8.61:50070', root='/', timeout=10)
    file_local = '{0}'.format(local)
    try:
        with client.read(hdfs_location) as r:
            with open(file_local, 'wb') as f:
                f.write(r.read())

    except (hdfs.util.HdfsError, IOError) as msg:
        print(msg)
        file_local = None

    return file_local

