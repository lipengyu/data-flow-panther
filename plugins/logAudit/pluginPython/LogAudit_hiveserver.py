from _datetime import datetime
import http.client
import urllib.request

import requests
import time

from bs4 import BeautifulSoup
import html5lib


dict = []
queryTime = ''
topic = 'log_audit_hiveserver2'

#解析hive的监控页面，返回html脚本
def getContent(url):
    d = requests.get(url)
    return BeautifulSoup(d.content,"html5lib")

#给http发送从url获取到的字段，发送给kafka的topic
def send_data(TData):
    conn = http.client.HTTPConnection("kafka.waterelephant.cn")
    headers = {
        'content-type': "application/json",
        'topic': topic,
        'token': "bigdata.hive",
    }
    conn.request("POST", "/rest/api/msg", TData, headers)
    res = conn.getresponse()
    data = res.read()
    conn.close()

#得到每一台服务的信息
#每次抓取的是最近半小时的query  用时间戳来判断   nowtimesatamp - openedtimestamp <= 30*1000
def hiveserver2_session(host):
    while True:
        time.sleep(60)
        hiveJsp = 'http://'+host+':10002/hiveserver2.jsp'
        soup = getContent(hiveJsp)
        for h2 in soup.findAll('h2'):
            if h2.string == 'Open Queries':
                tab = h2.parent.find('table')
                for tr in tab.findAll('tr'):
                    if tr.find('a') != None:
                        count = 0
                        for td in tr.findAll('td'):
                            if count == 0:
                                dict = str(td.getText()).strip()
                                dict = dict+'^'
                            elif count == 1:
                                dict = dict + str(td.getText()).strip()
                                dict = dict + '^'
                            elif count == 2:
                                dict = dict + str(td.getText()).strip()
                                dict = dict + '^'
                            elif count == 3:
                                dict = dict + str(td.getText()).strip()
                                dict = dict + '^'
                            elif count == 4:
                                dt = td.getText().strip()
                                queryTime = int(time.mktime(time.strptime(dt, '%a %b %d %H:%M:%S CST %Y')))
                                timestamp = str(queryTime)
                                dict = dict + timestamp
                                dict = dict + '^'
                            elif count == 5:
                                dict = dict + str(td.getText()).strip()
                                dict = dict + '^'
                            elif count == 6:
                                dict = dict + str(td.getText()).strip()
                                dict = dict + '^'
                            else:
                                #'http://10.50.40.4:10002/query_page?operationId'=+tr.find('a').get('href').split('=')[1]
                                dict = dict + 'http://'+host+':10002/query_page?operationId='+tr.find('a').get('href').split('=')[1]
                            count += 1
                        temp = str(dict.encode('utf-8')).replace("\n", " ")
                        oldN = r'\n'
                        oldR = r'\r'
                        oldK = r'         '
                        temp = temp.replace(str(oldN), " ")
                        temp = temp.replace("    ", " ")
                        temp = temp.replace(oldK," ")
                        temp = temp.replace(str(oldR), " ")
                        # 获取当前时间并转为时间数组
                        times = time.strptime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "%Y-%m-%d %H:%M:%S")
                        # 转为时间戳
                        timeStamp = int(time.mktime(times))
                        if (timeStamp-queryTime)<= 60:
                            send_data(temp)
                            print(temp)

def getData():
    for i in range(4, 11):
        hostName = "10.50.40.{}".format(i)
        hiveserver2_session(hostName)

if __name__ == "__main__":
    getData()


