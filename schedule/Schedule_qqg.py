#!/usr/bin/env python3
import sys
#from app_vdp_wric_user_behvr_cate_inner_new_sku_trend_month_rank_v2 import UserBehCateInnerNewSkuTrendMonthRank
import Decorator
from HiveTask import HiveTask
import multiprocessing as mp
from Daily_Monthly_Job_qqg import daily_job,monthly_job  #这里不用改
import threading
#使用方法
#sys.argv[1] 传参开始日期
#sys.argv[2] 传参结束日期
#sys.argv[3] 传参并发数量
#sys.argv[4] 传参bd_id
#在Daily_Monthly_Job里import脚本和类
#并在daily_job或month方法下加上跑脚本的代码,如果有bd_id,就传入date_list[2],若没有就不传
#last_year_mthend = [[2016-01-31,'PGX'],till the end of last year]
#current_year_mthend = [[2017-01-31,'PGX'],till the end of this year]
#PGX represents all bd_id input as argument
class Schedule_qqg(threading.Thread):
    def __init__(self,start_date,end_date,pool,bd_id):
        super(Schedule_qqg, self).__init__()
        print("inner_cate品类内拉新开始运行...")
        self.start_date = start_date
        self.end_date = end_date
        self.bd_id = bd_id
        self.parallel = pool
    @Decorator.Daily_deco
    def schedule_daily(date_list,pool):
        p = mp.Pool(pool)
        p.map(daily_job,date_list)

    @Decorator.Month_deco
    def schedule_month(date_list,pool):
        p = mp.Pool(pool)
        p.map(monthly_job, date_list)
    def run(self):
        self.schedule_daily()
        self.schedule_month()
        print("品类内拉新模块执行成功")
#def 新模块
if __name__ == '__main__':
    if sys.argv[1] == '-h':
        print ("par1:start_date\npar2:end_date\npar3:parallel\npar4:bd_id")
    else:
        schedule_daily()
        schedule_month()
        print ("开始日期:%s\n结束日期:%s" % (sys.argv[1],sys.argv[2]))
    #其他模块也def函数,然后在这里运行,日和月的分开写,命名为自己模块_daily/monthly
