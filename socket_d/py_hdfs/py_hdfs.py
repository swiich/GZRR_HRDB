import hdfs
import os

# upload()函数中Progress-回调函数跟踪进度
# length指定读取字节数长度
# chunk_size指定每次读取字节数，返回generator，使文件内容变为流数据


def download_file(hdfs_location, file_local):
    """
    :param hdfs_location: hdfs路径+文件名
    :param file_local: 本地路径+文件名
    """
    # 可以用 try finally提高可读性
    client = hdfs.Client('http://172.39.8.61:50070', root='/', timeout=10)
    try:
        with client.read(hdfs_location) as r:
            with open(file_local, 'wb') as f:
                f.write(r.read())

    except hdfs.util.HdfsError:
        client = hdfs.Client('http://172.39.8.62:50070', root='/', timeout=10)
        with client.read(hdfs_location) as r:
            with open(file_local, 'wb') as f:
                f.write(r.read())

    except IOError as msg:
        with open("err.log", "a") as f:
            f.write(str(msg))
        file_local = None

    return file_local


def upload_file(hdfs_location, local):
    try:
        client = hdfs.Client('http://172.39.8.61:50070', root='/', timeout=10)
        base_dir = local.split('/').pop()  # 要上传的路径的最后一个文件夹

        for root, dirs, files in os.walk(local):

            new_dir = base_dir + root.split(base_dir).pop()  # 去除本地路径前缀

            for file in files:
                old_path = root + '/' + file  # 原始本地路径文件
                lpath = new_dir + '/' + file  # 去除本地路径前缀后的文件

                if not client.status(hdfs_location + '/' + lpath, strict=False):
                    # 第一个参数远程路径，第二个参数本地路径，第三个参数是否覆盖，第四个参数工作线程数
                    client.upload(hdfs_location + '/' + lpath, old_path, overwrite=False)

    except Exception as e:
        with open("err.log", "a") as f:
            f.write(str(e))
            f.write('\n')
