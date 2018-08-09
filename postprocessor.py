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
import cookiejar
import ssl
import xlrd
def fill_sheet():
    sheet = xlrd.open_workbook('Summary.xlsx').sheet_by_name('Sheet1')
    with open('output.txt', 'r') as file_in:
        res = file_in.readlines()

    temp = [_.split('\t') for _ in res]
    res = {}
    for _ in temp:
        if _[-1] == 'empty\n':
            res[_[1]] = 'empty'
        elif _[-1] == 'error\n':
            pass
        elif _[1] not in res:
            res[_[1]] = {_[0]: _[7:]}
        else:
            res[_[1]][_[0]] = _[7:]


    out_temp = [''] * sheet.nrows

    for _ in range(1, sheet.nrows):
        row = sheet.row_values(_)
        try:
            year = str(int(row[3] + 0.1))
            month_day = str(int(row[4] + 0.1))
            month_day = ('0' if len(month_day) == 3 else '') + month_day
            date = year + month_day
            xls_price = float(row[6])
            pin = str(int(row[15] + 0.1))

            if pin in res:
                if res[pin] == 'empty':
                    out_temp[_ - 1] = '0\n'
                else:
                    if date in res[pin]:
                        date_epoch = time.mktime(time.strptime(date, "%Y%m%d"))
                        p_epoch = time.mktime(time.strptime(res[pin][date][0], "%m/%d/%Y"))
                        if abs(date_epoch - p_epoch) > 60 * 60 * 24 * 60:
                            out_temp[_ - 1] = '0\n'
                        elif abs(xls_price - float(res[pin][date][2][1:])) > 10:
                            out_temp[_ - 1] = '1\n'
                        else:
                            out_temp[_ - 1] = '2\n'
            else:
                out_temp[_ - 1] = '\n'
        except:
            traceback.print_exc()
            print(row)
            input()
            out_temp[_ - 1] = '\n'

    file_out = open('mls.txt', 'w')
    file_out.writelines(out_temp)
    file_out.close()


def get_unprocess():
    sheet = xlrd.open_workbook('Summary.xlsx').sheet_by_name('Sheet1')
    file_out = open('uprocess.txt', 'w')
    for _ in range(1, sheet.nrows):
        row = sheet.row_values(_)
        if isinstance(row[-1], str):
            year = str(int(row[3] + 0.1))
            month_day = str(int(row[4] + 0.1))
            month_day = ('0' if len(month_day) == 3 else '') + month_day
            row = [year + month_day] + [row[-_] if isinstance(row[-_], str) else str(int(row[-_] + 0.1)) for _ in reversed(range(2, 9))]
            row = ','.join(row[:3] + row[4:])
            file_out.write(row + '\n')
    file_out.close()
# fill_sheet()
get_unprocess()