#!/usr/bin/env python3
# 用法: ./main_job.py 2017-02-01 bd_id1,bd_id2,bd_id3 将自动跑2017年1月31日的三个bd_id的画像数据
import sys
import time
import threading
import datetime
import calendar

#sys.path.append('common/')
sys.path.insert(0,'/home/mart_vdp/lxztest/cx/portrait/common')
from datetime_util import DateTimeUtil
from HiveTask import HiveTask
from parse_bd_id import parse_bd_id

from app_vdp_wric_user_sales_portrait_v3 import Portrait  # portrait表
from app_vdp_wric_user_sales_portrait_all_v3 import PORTRAITALL  # 品类画像表-由上一个临时表脱去用户维度汇总而来
from app_vdp_wric_user_sales_portrait_compare_v3 import PORTRAITCOMPARE  # 品牌商下品牌用户画像表,同比表compare
from app_vdp_wric_user_sales_portrait_ration_v3 import PORTRAITRATION  # 品牌商下品牌用户画像表,画像各属性比例表ration
from app_vdp_wric_user_sales_portrait_all_compare_v3 import ALLCOMPARE  # 全品类下用户画像表,同比表all_compare
from app_vdp_wric_user_sales_portrait_all_ration_v3 import ALLRATION  # 全品类下用户画像表,画像各属性比例表all_ration
from app_vdp_wric_user_sales_portrait_pre_v3 import PortraitPre
from app_vdp_wric_user_sales_portrait_all_compare_plus_v3 import ALLCOMPAREPLUS
from app_vdp_wric_user_sales_portrait_all_ration_plus_v3 import ALLRATIONPLUS
from app_vdp_wric_user_sales_portrait_compare_plus_v3 import PORTRAITCOMPAREPLUS
from app_vdp_wric_user_sales_portrait_ration_plus_v3 import PORTRAITRATIONPLUS
from app_vdp_wric_user_sales_portrait_kanban_plus_v3 import KANBANPLUS
from app_vdp_wric_user_sales_plus_user_v3 import PLUSUSER
from app_vdp_wric_user_sales_plus_amt_v3 import PLUSAMT
from app_vdp_wric_user_sales_plus_tb_v3 import PLUSTB
import multiprocessing as mp
import threading


def run_portrait_batch_base(date_list):
    jobdate = date_list[0]
    bd_id = date_list[1]
    ht = HiveTask()
    
    portraitpre = PortraitPre(jobdate, ht, bd_id)
    portraitpre.run()  #并发run_portrait_batch_base主函数时, 先并发run()函数,等run()的跑完1个,才会调起portall和portrait,且由于这两个是start并发,会同时调起run()刚结束的那个日期
    portraitall = PORTRAITALL(jobdate, ht, bd_id)
    portraitall.start()
    portrait = Portrait(jobdate, ht, bd_id)
    portrait.start()
    plusamt = PLUSAMT(jobdate, ht, bd_id)
    plusamt.start()
    # portrait结束
    portrait.join()
    # portrail_all结束
    portraitall.join()
    # plus用户模块amt表结束
    plusamt.join()
    print ("fundamental tables done!")


def run_portrait_batch_cur(date_list_cur_year):
    jobdate = date_list_cur_year[0]
    bd_id = date_list_cur_year[1]
    ht = HiveTask()
    # portrait后续开始
    portraitcompare = PORTRAITCOMPARE(jobdate, ht, bd_id)
    portraitration = PORTRAITRATION(jobdate, ht, bd_id)
    portraitcompare.start()
    portraitration.start()
    portraitcompareplus = PORTRAITCOMPAREPLUS(jobdate, ht, bd_id)
    portraitrationplus = PORTRAITRATIONPLUS(jobdate, ht, bd_id)
    portraitcompareplus.start()
    portraitrationplus.start()
    kanbanplus = KANBANPLUS(jobdate, ht, bd_id)
    kanbanplus.start()
    # portrait_all后续开始
    allcompare = ALLCOMPARE(jobdate, ht, bd_id)
    allration = ALLRATION(jobdate, ht, bd_id)
    allcompareplus = ALLCOMPAREPLUS(jobdate, ht, bd_id)
    allrationplus = ALLRATIONPLUS(jobdate, ht, bd_id)
    allcompareplus.start()
    allrationplus.start()
    allcompare.start()
    allration.start()
    allcompare.join()
    allration.join()
    portraitcompare.join()
    portraitration.join()
    portraitcompareplus.join()
    portraitrationplus.join()
    allcompareplus.join()
    allrationplus.join()
    kanbanplus.join()
    # plus用户模块后续开始
    plustb = PLUSTB(jobdate, ht, bd_id)
    plususer = PLUSUSER(jobdate, ht, bd_id)
    plustb.start()
    plususer.start()
    plustb.join()
    plususer.join()
    print ("All tables finished!")


class PORTRAIT_MAIN(threading.Thread):
    def __init__(self, start_dt,end_dt,pool, ht, bd_id=None):
        super(PORTRAIT_MAIN, self).__init__()
        print("user_por用户细分开始运行...")
        self.ht = ht
        self.bd_id = bd_id
        self.pool = int(pool)
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.start_dt_last = str(int(self.start_dt[0:4]) - 1) + self.start_dt[4:10]
        self.date_list = [[self.start_dt,self.bd_id]]
        self.date_list_last = [[self.start_dt_last,self.bd_id]]
        self.date = datetime.datetime.strptime(start_dt, '%Y-%m-%d')
        self.date_last = datetime.datetime.strptime(str(int(self.date.strftime('%Y-%m-%d')[0:4])-1 )+self.date.strftime('%Y-%m-%d')[4:10],'%Y-%m-%d')
        self.date_list_last_year = []

        while self.date_last.strftime('%Y-%m-%d') < self.date.strftime('%Y-%m-%d'):
            self.date_list_last_year.append([self.date_last.strftime('%Y-%m-%d'), self.bd_id])
            firstday, monthend = calendar.monthrange(self.date_last.year, self.date_last.month)
            self.date_last = datetime.date(year=self.date_last.year, month=self.date_last.month, day=monthend) + datetime.timedelta(1)

        while self.date.strftime('%Y-%m-%d') < self.end_dt:
            firstday, monthend = calendar.monthrange(self.date.year, self.date.month)
            self.date = datetime.date(year=self.date.year, month=self.date.month, day=monthend) + datetime.timedelta(1)
            self.date_list.append([self.date.strftime('%Y-%m-%d'),self.bd_id])
            self.date_format = self.date.strftime('%Y-%m-%d')
            self.date_last = str(int(self.date_format[0:4]) - 1) + self.date_format[4:10]
            self.date_list_last.append([self.date_last,self.bd_id])

        print ("------------------当前年日期,给结果表用的-----------------\n")
        print (self.date_list)
        print ("日期分割线")
        #print(self.date_list_last)




    def run(self):
        # date_list = []
        # date_list_cur_year = []
        # while self.process_date <= self.cur_date:  # modified by lxz at 20171018
        #     date_list.append([str(self.process_date), self.bd_id])
        #     if self.process_date >= self.cur_year_start:
        #         date_list_cur_year.append([str(self.process_date), self.bd_id])
        #     print ("this is current process date: %s" % self.process_date)  # modified by lxz at 20171018
        #     firstday, monthend = calendar.monthrange(self.process_date.year,self.process_date.month)  # modified by lxz at 20171018
        #     print ("first day is: %s, monthend is: %s" % (firstday, monthend))  # modified by lxz at 20171018
        #     self.process_date = datetime.date(year=self.process_date.year, month=self.process_date.month,day=monthend) + datetime.timedelta(1)
        #     print ("this is new process date: %s" % self.process_date)
        # print (date_list)
        # print (date_list_cur_year)
        date_list = self.date_list
        #date_list_last = self.date_list_last + self.date_list
        date_list_last1 = self.date_list_last_year + self.date_list

        print ("----------------给portrait用的日期,包括当前年和同比的日期---------------------\n")
        #print (date_list_last)
        print ('新日期')
        print (date_list_last1)
        p = mp.Pool(self.pool)
        #result_list = p.map(run_portrait_batch_base, date_list_last)
        result_list = p.map(run_portrait_batch_base, date_list_last1)  #跑去年至今的所有数据, 确保新增品牌商后, 下个月正常跑数时, 去年同期有数据 20180605
        result_list_cur = p.map(run_portrait_batch_cur, date_list)
        print ("************************************")
        print ("All Done!")


def main():
    ht = HiveTask()
    bd_id = None
    if len(sys.argv) == 5:
        start_dt = sys.argv[1]
        end_dt = sys.argv[2]
        pool = sys.argv[3]
        bd_id = sys.argv[4]
    if len(sys.argv) == 4:
        start_dt = sys.argv[1]
        end_dt = sys.argv[2]
        pool = sys.argv[3]
        bd_id = None
    main_job = PORTRAIT_MAIN(start_dt,end_dt, pool,ht, bd_id)
    main_job.run()


if __name__ == '__main__':
    if sys.argv[1] == '-h':
        print("参数1:开始日期\n参数2:结束日期\n参数3:并发量\n参数4:品牌商\n将自动跑限定开始结束日期之间的用户细分结果")
    else:
        stime = time.time()
        main()
        etime = time.time()
        diff = (etime - stime) / 60
        print ("Time: %sminutes" % diff)
        print ("开始日期:%s\n结束日期:%s\n品牌商个数:%s" % (sys.argv[1],sys.argv[2],len(sys.argv[4].split(','))))
