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
from user_behv_main_job_v3 import BEHV_MAIN  # 用户升级调度
from main_job_portrait_v3 import PORTRAIT_MAIN  # 用户细分调度
from main_job_path_v2 import PATH_MAIN  #购物路径调度
from Schedule import Schedule
import multiprocessing as mp


class INTE_HEAD_JOB:
    def __init__(self,bd_id):
        self.ht = HiveTask()
        #self.jobDate = jobDate
        self.bd_id = bd_id
        self.cur_date = datetime.date.today()
        self.cur_date_str = self.cur_date.strftime('%Y-%m-%d')
        self.last_month_start = DateTimeUtil.oneday_by_date(self.cur_date_str[0:8] + '01',-1, '%Y-%m-%d')[0:8]+'01'
        self.year_start = self.cur_date.strftime('%Y-%m-%d')[0:4]+'-02-01'  #结果为2018-02-01,故跑数start_date为2018-01-31,年初第一月
        self.cur_month = self.cur_date.strftime('%Y-%m-%d')[0:8]+'01'  #调用此脚本时的日期所在月, 即跑数的end_date为当前月
        self.pool = 5
    def run_job(self):
        portrait_main = PORTRAIT_MAIN(self.year_start,self.cur_month,self.pool, self.ht, self.bd_id)
        behv_main = BEHV_MAIN(self.year_start,self.cur_month,self.ht,self.pool, self.bd_id)
        path_main = PATH_MAIN(self.cur_date_str, self.ht, self.pool,self.bd_id)  #购物路径,日期自己内部算, 与传参无关
        sche = Schedule(self.last_month_start,self.cur_date_str,self.pool,self.bd_id)
        portrait_main.start()      #用户细分
        behv_main.start()      #用户升级
        path_main.start()
        sche.start()
        sche.join()
        path_main.join()    
        portrait_main.join()    
        behv_main.join()    
  

    @staticmethod
    def do_main(input_bd_id):
        bd_id = input_bd_id
        main_job = INTE_HEAD_JOB(bd_id)
        main_job.run_job()

def main():
    bd_id = sys.argv[1]
    main_job = INTE_HEAD_JOB(bd_id)
    main_job.run_job()


if __name__ == '__main__':
    stime = time.time()
    main()
    etime = time.time()
    diff = (etime - stime) / 60
    print ("Time: %sminutes" % diff)