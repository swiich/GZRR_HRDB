from ftplib import FTP
import time
import tarfile
import os


def ftpconnect(host, username, password):
    ftp = FTP()
    ftp.connect(host, 21)
    ftp.login(username, password)
    return ftp

# 从ftp下载文件


def downloadfile(ftp, remotepath, localpath):
    bufsize = 1024
    local = localpath + '.tmp'
    fp = open(local, 'wb')
    ftp.retrbinary('RETR ' + remotepath, fp.write, bufsize)
    ftp.set_debuglevel(0)
    fp.close()
    os.rename(local, localpath)

# 取得新文件


def newfile(ftp, des_path):
    ftp.cwd(des_path)
    ftpfile = file_filter(ftp.nlst())
    newfile = []
    if not os.path.exists('downloadedfile'):
        downloadedfile = open('downloadedfile', 'w')
        downloadedfile.close()
    downloadedfile = open('downloadedfile', 'r')
    filelist = downloadedfile.readlines()
    for i, file in enumerate(filelist):
        filelist[i] = filelist[i][:-1]
    for file in ftpfile:
        if file not in filelist:
            newfile.append(file)
    downloadedfile.close()
    return newfile


def file_filter(filelist):
    """
    过滤列表中xml和bin，使其一一对应
    """
    file_bin_list = []
    file_xml_list = []
    res = []
    # split      可用列表推导式简化？
    for file in filelist:
        ext = file.split('.')[-1]
        if ext == 'bin':
            file_bin_list.append(file)
        elif ext == 'xml':
            file_xml_list.append(file)
    # match         
    for xmlfile in file_xml_list:
        binfile = str(xmlfile.split('.')[0]) + '.bin'
        if binfile in file_bin_list:
            res.append(xmlfile)
            res.append(binfile)

    return res


if __name__ == '__main__':
    ftp = ftpconnect('172.18.130.16', 'gzww', 'gzww')
    while True:
        newfile_list = newfile(ftp, 'RealtimeData')

        # 无新文件
        if not newfile_list:
            time.sleep(10)
            continue

        downloadedfile = open('downloadedfile', 'a')
        for file in newfile_list:
            downloadfile(ftp, file, 'D:\\HRDATA\\RealtimeData\\' + file)
            downloadedfile.write(file + '\n')
        downloadedfile.close()

        time.sleep(10)
