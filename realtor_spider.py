from faker import Faker
import urllib.request
import urllib.parse
import json
import time
from bs4 import BeautifulSoup
import random
import threading
import traceback
import socket
import http.cookiejar
import ssl
import xlrd
ssl._create_default_https_context = ssl._create_unverified_context
socket.setdefaulttimeout(60)
url_base = 'https://www.realtor.com'
fake = Faker('zh_CN')
proxy_list = [
              # 'NO PROXY',
              '127.0.0.1:1080',
              '127.0.0.1:1081',
              # '127.0.0.1:1082',
              '127.0.0.1:1083',
              '127.0.0.1:1084',
              '127.0.0.1:1085',
              '118.24.193.85:4545', # tencent
              '134.175.190.115:4545',#tencent
              '39.104.141.25:4545', #ali
              '39.104.142.30:4545', #ali


              #'132.232.129.147:4545', # tencent
              # '62.234.169.182:4545',#tencent
              # '62.234.168.142:4545',#tencent
              # '62.234.203.222:4545',#tencent
              #'39.104.141.148:4545', #ali
              #'212.64.107.134:4545', #tencent
              #'118.25.228.44:4545', # tencent
              # '120.78.166.169:4545',
              # '154.8.140.57:4545', tencent
              # '118.24.136.63:4545' tencent
              # '47.100.176.75' tencent
              # '39.104.141.111' tencent
              # '39.105.45.29' tencent
              #'159.138.2.226' huawei
              #'159.138.6.92' huawei
              ]


def get_one(data, opener):
    data = data.split(',')
    if data[-2] == '' or (data[-2].isdigit() and int(data[-2]) == 0):
        data = data[:-2] + data[-1:]
    search_word = ','.join(data[2:])
    search_word = ','.join(data[2:]).replace(' ', '+')
    url = '/validate_geo?location=' + search_word + '&prop_status=for_sale&retain_secondary_facets=true&include_zip=false&search_controller=Search%3A%3APropertiesController'
    req = opener.open(url_base + url).read().decode('utf-8')
    redirect_url = json.loads(req)['url']
    ret_html = opener.open(url_base + redirect_url).read().decode('utf-8')
    bs = BeautifulSoup(ret_html, 'html5lib')
    bs_history_price = bs.find(id='ldp-history-price')
    bs_history_price = bs_history_price.find_all('tr')
    sold_history = []
    for _ in bs_history_price:
        if _.find(text='Sold'):
            sold_history.append([i.text for i in _.find_all('td')])

    for i in range(len(sold_history)):
        for j in range(len(sold_history[i])):
            sold_history[i][j] = ''.join(sold_history[i][j].split(','))
    # sold_history = sorted(sold_history,
    #                       key=lambda _: abs(time.mktime(time.strptime(data[:8], "%Y%m%d")) - time.mktime(time.strptime(_[0], "%m/%d/%Y"))))
    return sold_history


def proxy_test(opener_list):
    for _ in opener_list:
        _ = _[0]
        good_proxy = False
        try:
            if isinstance(get_one("20141229,163992351,9174,MARTIN LUTHER KING J,BLVD,0,DENVER", _), list):
                print(_, 'OK!')
                good_proxy = True
        except:
            traceback.print_exc()
            pass
        finally:
            if not good_proxy:
                print(_, 'BROKEN!')


def thread_func(opener_, job_by_pin, job, job_lock, out_file, out_file_lock):
    opener = opener_[0]
    while job:
        job_lock.acquire()
        now = job.pop()
        job_lock.release()

        write_lines = []
        ret = None
        try:
            ret = get_one(now, opener)
        except:
            #traceback.print_exc()
            if opener_[1] == 'NO PROXY':
                proxy_handler = urllib.request.ProxyHandler()
            else:
                proxy_handler = urllib.request.ProxyHandler({'http': opener_[1], 'https': opener_[1]})
            opener = urllib.request.build_opener(proxy_handler, urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
            opener.addheaders = [('User-agent', fake.user_agent())]


        if ret is None:
            write_lines.append('\t'.join(now.split(',')) + '\terror\n')
            print('error occurred :', now)
        else:
            pin = now.split(',')[1]
            for i in all_job_by_pin[pin]:
                ret = sorted(ret, key=lambda _: abs(time.mktime(time.strptime(i.split(',')[0], "%Y%m%d")) - time.mktime(time.strptime(_[0], "%m/%d/%Y"))))
                write_lines.append('\t'.join(i.split(',')) + '\t' + ('\t'.join(ret[0]) if ret else 'empty') + '\n')

        out_file_lock.acquire()
        out_file.writelines(write_lines)
        out_file.flush()
        out_file_lock.release()
        time.sleep(6)

if __name__ == '__main__':
    opener_list = []
    for _ in proxy_list:
        if _ == 'NO PROXY':
            proxy_handler = urllib.request.ProxyHandler()
        else:
            proxy_handler = urllib.request.ProxyHandler({'http': _, 'https': _})
        opener = urllib.request.build_opener(proxy_handler, urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
        opener.addheaders = [('User-agent', fake.user_agent())]
        opener_list.append((opener, _))

    proxy_test(opener_list)
    input('Continue?')
    with open('trimmed_data.txt', 'r') as in_file:
        all_job = in_file.readlines()

    all_job = reversed([_.strip('\n') for _ in all_job])
    all_job_by_pin = dict()
    temp = []
    for _ in all_job:
        pin = _.split(',')[1]
        if pin in all_job_by_pin:
            all_job_by_pin[pin].append(_)
        else:
            temp.append(_)
            all_job_by_pin[pin] = [_]
    all_job = temp
    job_lock = threading.Lock()

    out_file = open('output.txt', 'a')
    out_file_lock = threading.Lock()

    for _ in opener_list:
        threading.Thread(target=thread_func, args=(_, all_job_by_pin, all_job, job_lock, out_file, out_file_lock)).start()

    while all_job:
        print(time.asctime(), 'COUNTER :', len(all_job))
        time.sleep(10)
            
    out_file.close()




# def trim_sheet():
#     sheet = xlrd.open_workbook('Summary.xlsx').sheet_by_name('Sheet1')
#     file_out = open('trimmed_data.txt', 'w')
#     for _ in range(1, sheet.nrows):
#         row = sheet.row_values(_)
#         year = str(int(row[3] + 0.1))
#         month_day = str(int(row[4] + 0.1))
#         month_day = ('0' if len(month_day) == 3 else '') + month_day
#         row = [year + month_day] + [row[-_] if isinstance(row[-_], str) else str(int(row[-_] + 0.1)) for _ in reversed(range(2, 9))]
#         row = ','.join(row[:3] + row[4:])
#         file_out.write(row + '\n')
#     file_out.close()

# trim_sheet()