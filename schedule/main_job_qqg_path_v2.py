#!/usr/bin/env python3
# 用法: ./main_job.py 2017-02-01 bd_id1,bd_id2,bd_id3 将自动跑2017年1月31日的三个bd_id的画像数据
import sys
import time
import threading
import datetime
import calendar

sys.path.append('common/')
from datetime_util import DateTimeUtil
from HiveTask import HiveTask
from parse_bd_id import parse_bd_id
from app_vdp_wric_qqg_user_path_data_v2 import QQGDATATREND  # path_data表
from app_vdp_wric_qqg_user_path_keyword_v2 import QQGKEYWORD  # 关键字日表
import multiprocessing as mp
import threading

def run_path_daily(date_list):
    jobdate = date_list[0]
    bd_id = date_list[1]
    ht = HiveTask()
    datatrend = QQGDATATREND(jobdate, ht, bd_id)
    datatrend.run()
    print("PATH finished!")


def run_keyword_daily(date_list):
    jobdate = date_list[0]
    ht = HiveTask()
    keyword_day = QQGKEYWORD(jobdate, ht)
    keyword_day.run()
    print("KEYWORD done. All finished!")
# def run_keyword_monthly(date_list_month_start):
#     jobdate = date_list_month_start
#     ht = HiveTask()
#     keyword_month = KEYWORD_MONTH(jobdate, ht)
#     keyword_month.run()





class PATH_MAIN_QQG(threading.Thread):
    def __init__(self, end_date, ht, pool,bd_id=None):
        super(PATH_MAIN_QQG, self).__init__()
        print ("shopping_path全球购购物路径开始运行...")
        self.ht = ht
        self.end_date = end_date
        self.cur_date = datetime.date.today()  # modified by lxz at 20171018
        self.process_date=datetime.date(year=self.cur_date.year,month=self.cur_date.month,day=self.cur_date.day) + datetime.timedelta(-30)
        self.bd_id = bd_id
        self.pool = int(pool)

    def run(self):
        date_list = []
        while self.process_date.strftime('%Y-%m-%d') <= self.cur_date.strftime('%Y-%m-%d'):  # modified by lxz at 20171018
            date_list.append([str(self.process_date), self.bd_id])
            print("this is current process date: %s" % self.process_date)  # modified by lxz at 20171018
            self.process_date += datetime.timedelta(1)
            print("this is new process date: %s" % self.process_date)

        print(date_list)
        p = mp.Pool(self.pool)
        datatrend = p.map(run_path_daily, date_list)
        keyword_day = p.map(run_keyword_daily, date_list)
        print("************************************")
        print("All Done!")





