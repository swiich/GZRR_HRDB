
import http.client
from html import unescape
import xml.etree.cElementTree as Et
import csv


def get_res():
    query = '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/">\
       <soapenv:Header>\
          <tem:MonitorHeader>\
             <!--Optional:-->\
             <tem:TransId>fsf</tem:TransId>\
             <!--Optional:-->\
             <tem:BizKey>sdwrwr</tem:BizKey>\
             <!--Optional:-->\
             <tem:PSCode>PS-140000-01-0001-0045</tem:PSCode>\
             <!--Optional:-->\
             <tem:BSCode>BS-140000-01-0001-0045</tem:BSCode>\
          </tem:MonitorHeader>\
       </soapenv:Header>\
       <soapenv:Body>\
          <tem:QueryService1>\
             <!--Optional:-->\
             <tem:cXML><![CDATA[<?xml version="1.0" encoding="utf-8" ?>\
    <TEMPLATE ORDERBY="申请表编号(1),资料表编号(0)" RESULTTYPE="0" PAGEINDEX="" PAGESEIZE="">\
    <CONDITIONS>   \
       <COLUMN DISPLAYNAME="频率" FREQ_TYPE="0" PRESYMBOL="" MATHSYMBOL="" CONTAINBOUND1="0" CONTAINBOUND2="0" VALUE1="880" VALUE2="980" LOGICRELATION="0" LEFTBRACKET="" RIGHTBRACKET="" />\
    </CONDITIONS>\
    <RESULTLIST>\
       <COLUMN DISPLAYNAME="行政区" />\
       <COLUMN DISPLAYNAME="网络名称" />\
       <COLUMN DISPLAYNAME="通信系统" />\
       <COLUMN DISPLAYNAME="台站地址" />\
       <COLUMN DISPLAYNAME="台站名称" />\
       <COLUMN DISPLAYNAME="台站地址" />\
       <COLUMN DISPLAYNAME="申请单位" />\
       <COLUMN DISPLAYNAME="台站状态" />\
       <COLUMN DISPLAYNAME="发射频率"/>\
       <COLUMN DISPLAYNAME="台站经度"/>\
       <COLUMN DISPLAYNAME="台站纬度"/>\
       <COLUMN DISPLAYNAME="功率"/>\
       <COLUMN DISPLAYNAME="服务半径"/>\
       <COLUMN DISPLAYNAME="资料表类型"/>\
       <COLUMN DISPLAYNAME="技术体制"/>\
    </RESULTLIST>\
    </TEMPLATE>]]></tem:cXML>\
          </tem:QueryService1>\
       </soapenv:Body>\
    </soapenv:Envelope>\
    '.encode()

    webservice = http.client.HTTPConnection('172.18.140.6:7000')
    webservice.putrequest('POST', '/QueryService.asmx')
    webservice.putheader("User-Agent", "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0)")
    webservice.putheader("Content-type", "text/xml; charset=\"UTF-8\"")
    webservice.putheader("Content-length", "%d" % len(query))
    webservice.endheaders()
    webservice.send(query)

    res = webservice.getresponse().read().decode()

    return unescape(res)


if __name__ == '__main__':

    root = Et.parse('res1.xml').getroot()
    rows = root.getchildren()
    file = open('station.csv', 'a', newline='')

    for i, row in enumerate(rows):
        if i == len(rows) - 1:
            first_row = rows[i]
            # 判断距离单位
            tmp = first_row.get('ST_SERV_R')
            # 判断是否为空
            if not tmp:
                continue
            if first_row.get('STAT_APP_TYPE') != 'D':
                a = first_row.get('ST_SERV_R')
                st_serv_r = float(first_row.get('ST_SERV_R')) * 1000
            else:
                st_serv_r = float(first_row.get('ST_SERV_R'))

            # 判断功率
            if first_row.get('STAT_APP_TYPE') == 'B':
                equ_pow = first_row.get('FT_FREQ_EPOW')
            else:
                equ_pow = first_row.get('EQU_POW')

            writer = csv.writer(file)
            writer.writerow((first_row.get('APP_CODE'),first_row.get('NET_NAME'),first_row.get('NET_SVN'),
                            first_row.get('GUID'),first_row.get('STAT_ADDR'),first_row.get('STAT_NAME'),first_row.get('APP_CODE')[1:5]+'00',
                            first_row.get('ORG_NAME'),first_row.get('STAT_STATUS'),first_row.get('FREQ_EFB')+'-'+first_row.get('FREQ_EFE'),
                            equ_pow,first_row.get('STAT_LA'),first_row.get('STAT_LG'),first_row.get('NET_TS'),st_serv_r))
        else:

            first_row = rows[i]
            next_row = rows[i+1]
            if first_row.get('GUID') == next_row.get('GUID'):
                continue
            else:
                # 判断距离单位
                tmp = first_row.get('ST_SERV_R')
                # 判断是否为空
                if not tmp:
                    continue
                if first_row.get('STAT_APP_TYPE') != 'D':
                    a = first_row.get('ST_SERV_R')
                    st_serv_r = float(first_row.get('ST_SERV_R')) * 1000
                else:
                    st_serv_r = float(first_row.get('ST_SERV_R'))

                # 判断功率
                if first_row.get('STAT_APP_TYPE') == 'B':
                    equ_pow = first_row.get('FT_FREQ_EPOW')
                else:
                    equ_pow = first_row.get('EQU_POW')

                writer = csv.writer(file)
                writer.writerow((first_row.get('APP_CODE'),first_row.get('NET_NAME'),first_row.get('NET_SVN'),
                                first_row.get('GUID'),first_row.get('STAT_ADDR'),first_row.get('STAT_NAME'),first_row.get('APP_CODE')[1:5]+'00',
                                first_row.get('ORG_NAME'),first_row.get('STAT_STATUS'),first_row.get('FREQ_EFB')+'-'+first_row.get('FREQ_EFE'),
                                equ_pow,first_row.get('STAT_LA'),first_row.get('STAT_LG'),first_row.get('NET_TS'),st_serv_r))

    file.close()
