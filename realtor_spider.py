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
ssl._create_default_https_context = ssl._create_unverified_context
socket.setdefaulttimeout(60)
url_base = 'https://www.realtor.com'
fake = Faker('zh_CN')
proxy_list = [
              'NO PROXY',
              #'127.0.0.1:1080',
              '127.0.0.1:1081',
              #'127.0.0.1:1082',
              '127.0.0.1:1083',
              #'127.0.0.1:1084',
             # '127.0.0.1:1085',
              '127.0.0.1:1086',
              '127.0.0.1:1087',
              '127.0.0.1:1088',
              '127.0.0.1:1089',
              #'127.0.0.1:1090',
              #'127.0.0.1:1091'
              ]


def get_one(data, opener):
    url = '/validate_geo?location=' + data[9:] + '&prop_status=for_sale&retain_secondary_facets=true&include_zip=false&search_controller=Search%3A%3APropertiesController'
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
    sold_history = sorted(sold_history,
                          key=lambda _: abs(time.mktime(time.strptime(data[:8], "%Y%m%d")) - time.mktime(time.strptime(_[0], "%m/%d/%Y"))))
    return sold_history


def proxy_test(opener_list):
    for _ in opener_list:
        good_proxy = False
        try:
            if get_one("20090327,2616,SHERIDAN,BLVD,0,DENVER", _):
                print(_, 'OK!')
                good_proxy = True
        except:
            traceback.print_exc()
            pass
        finally:
            if not good_proxy:
                print(_, 'BROKEN!')


def thread_func(opener, job, job_lock, out_file, out_file_lock):
    while job:
        job_lock.acquire()
        now = job.pop()
        job_lock.release()

        write_line = ''
        try:
            ret = get_one(now, opener)
            write_line = '\t'.join(now.split(',')) + '\t' + ('\t'.join(ret[0]) if ret else 'empty')
        except:
            traceback.print_exc()
            pass
        if not write_line:
            write_line = '\t'.join(now.split(',')) + '\terror'
            print('error occurred :', now)

        out_file_lock.acquire()
        out_file.write(write_line + '\n')
        out_file.flush()
        out_file_lock.release()

if __name__ == '__main__':
    opener_list = []
    for _ in proxy_list:
        if _ == 'NO PROXY':
            proxy_handler = urllib.request.ProxyHandler()
        else:
            proxy_handler = urllib.request.ProxyHandler({'http': _, 'https': _})
        opener = urllib.request.build_opener(proxy_handler, urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
        opener.addheaders = [('User-agent', fake.user_agent())]
        opener_list.append(opener)

    proxy_test(opener_list)
    input('Continue?')
    with open('trimmed_data.txt', 'r') as in_file:
        all_job = in_file.readlines()

    all_job = reversed([_.strip('\n') for _ in all_job])
    job_lock = threading.Lock()

    out_file = open('output.txt', 'a')
    out_file_lock = threading.Lock()

    for _ in opener_list:
        threading.Thread(target=thread_func, args=(_, all_job, job_lock, out_file, out_file_lock)).start()

    while all_job:
        print(time.asctime(), 'COUNTER :', len(all_job))
        time.sleep(20)
            
    out_file.close()




# def trim_sheet():
#     sheet = xlrd.open_workbook('Summary.xlsx').sheet_by_name('Sheet1')
#     file_out = open('trimmed_data.txt', 'w')
#     for _ in range(1, sheet.nrows):
#         row = sheet.row_values(_)
#         year = str(int(row[3] + 0.1))
#         month_day = str(int(row[4] + 0.1))
#         month_day = ('0' if len(month_day) == 3 else '') + month_day
#         row = [year + month_day] + [row[-_] if isinstance(row[-_], str) else str(int(row[-_] + 0.1)) for _ in reversed(range(2, 8))]
#         row = ','.join(row[:2] + row[3:])
#         file_out.write(row + '\n')
#     file_out.close()
