#!/usr/bin/env python3
import sys
import time
import calendar
import datetime
import threading

sys.path.append('common/')
from datetime_util import DateTimeUtil
from HiveTask import HiveTask
from parse_bd_id import parse_bd_id
import multiprocessing as mp
from app_vdp_wric_user_behvr_new_old_customer_base_all_cross_v3 import BASEALL
from app_vdp_wric_user_behvr_new_old_customer_base_bd_cross_v3 import BASEBDCROSS
from app_vdp_wric_user_behvr_new_old_customer_bd_cross_lost_v3 import BEHVRNEWOLDCROSSLOST
from app_vdp_wric_user_behvr_new_old_customer_bd_cross_new_v3 import BEHVRNEWOLDCROSSNEW
from app_vdp_wric_user_behvr_new_old_customer_bd_cross_remain_v3 import BEHVRNEWOLDCROSSREMAIN
from app_vdp_wric_user_behvr_new_old_customer_v3 import NEWOLDCUSTOMER
from app_vdp_wric_user_behvr_new_old_customer_result_da_v3 import RESULTDA
from app_vdp_wric_user_behvr_remained_action_v3 import REMAINEDACTION
from app_vdp_wric_user_behvr_top_ten_sku_v3 import TOPTEN
from app_vdp_wric_user_behvr_user_remained_v3 import USERREMAINED
from app_vdp_wric_user_behvr_new_user_source_jdnew_v3 import JDNEW
from app_vdp_wric_user_behvr_new_user_source_brandnew_v3 import BRANDNEW
from app_vdp_wric_user_behvr_new_user_source_catenew_v3 import CATENEW
from app_vdp_wric_user_behvr_new_user_source_v3 import USERSOURCE
from app_vdp_wric_user_behvr_new_old_customer_cur_mon_user_v3 import CUR_MON_USER
from app_vdp_wric_user_behvr_remained_action_cur_v3 import REMAINEDACTIONCUR
from app_vdp_wric_user_behvr_new_old_customer_result_da_cur_v3 import RESULTDACUR
from app_vdp_wric_user_behvr_user_remained_cur_v3 import USERREMAINEDCUR
from app_vdp_wric_user_behvr_new_old_customer_bd_cross_lost_curmon_v3 import BEHVRNEWOLDCROSSLOSTCUR
from app_vdp_wric_user_behvr_top_ten_sku_plus_v3 import TOPTENPLUS
from app_vdp_wric_user_behvr_remained_action_plus_v3 import REMAINEDACTIONPLUS
from app_vdp_wric_user_sales_plus_newreg_v3 import NEWREGPLUS
ht = HiveTask()
def bdcross(date_list):
    basebdcross = BASEBDCROSS(date_list[0],ht,date_list[1])
    baseall = BASEALL(date_list[0],ht,date_list[1])
    basebdcross.start()
    baseall.start()
    basebdcross.join()
    baseall.join()
def lost(date_list):
    lost = BEHVRNEWOLDCROSSLOST(date_list[0], ht, date_list[1])
    lost.run()
def new_and_lostcur(ht,date_list):
    new = BEHVRNEWOLDCROSSNEW(date_list[0],ht,date_list[1])
    new.start()
    lostcur = BEHVRNEWOLDCROSSLOSTCUR(date_list[0],ht,date_list[1])
    lostcur.start()
    new.join()
    lostcur.join()
def remain(ht,date_list):
    remain = BEHVRNEWOLDCROSSREMAIN(date_list[0],ht,date_list[1])
    remain.run()
def customer(ht,date_list):
    customer = NEWOLDCUSTOMER(date_list[0],ht,date_list[1])
    customer.run()
def customer_cur(ht,date_list):
    cuscur = CUR_MON_USER(date_list[0],ht,date_list[1])
    cuscur.run()
def res_tab(ht,date_list):
    res = RESULTDA(date_list[0],ht,date_list[1])
    #remact = REMAINEDACTION(date_list[0],ht,date_list[1])
    #topten = TOPTEN(date_list[0],ht,date_list[1])
    #userrem = USERREMAINED(date_list[0],ht,date_list[1])
    #jdnew = JDNEW(date_list[0],ht,date_list[1])
    #brandnew = BRANDNEW(date_list[0],ht,date_list[1])
    #catenew = CATENEW(date_list[0],ht,date_list[1])
    #usersource = USERSOURCE(date_list[0],ht,date_list[1])
    #remactcur = REMAINEDACTIONCUR(date_list[0],ht,date_list[1])
    #rescur = RESULTDACUR(date_list[0],ht,date_list[1])
    #userremcur = USERREMAINEDCUR(date_list[0],ht,date_list[1])
    #toptenplus = TOPTENPLUS(date_list[0], ht, date_list[1])
    #remainedactionplus = REMAINEDACTIONPLUS(date_list[0], ht, date_list[1])
    #newregplus = NEWREGPLUS(date_list[0], ht, date_list[1])
    #toptenplus.start()
    #remainedactionplus.start()
    #newregplus.start()
    res.start()
    #只要上一个函数并发的日期其中一个完成了,
    # 比如9月完成,这里的所有start同时并发9月,
    # 在pool中就不是设定的那么多了,
    # pool中的值可以理解为是传入函数的list中的并发量,
    # 而每一个并发都可以调起函数中的所有并发,
    # 所以注意一下总并发,别太多了
    #remact.start()
    #topten.start()
    #userrem.start()
    #jdnew.start()
    #brandnew.start()
    #remactcur.start()
    #rescur.start()
    #userremcur.start()
    #jdnew.join()
    #brandnew.join()
    #catenew.start()
    #catenew.join()
    #usersource.start()
    #usersource.join()
    res.join()
    #remact.join()
    #topten.join()
    #userrem.join()
    #remactcur.join()
    #rescur.join()
    #userremcur.join()
    #toptenplus.join()
    #remainedactionplus.join()
    #newregplus.join()


def seq_job(date_list):
    #ht = HiveTask()
    #new_and_lostcur(ht, date_list)
    #remain(ht, date_list)
    #customer(ht, date_list)
    #customer_cur(ht, date_list)
    res_tab(ht, date_list)
    #plus_job(ht, date_list)

def get_last_year_period(date):
    year = int(date[0:4] - 1)
    return str(year) + date[4:10]
class BEHV_MAIN(threading.Thread):
    def __init__(self, start_dt,end_dt, ht, pool,bd_id=None):
        super(BEHV_MAIN, self).__init__()
        print("user_behv用户升级开始运行...")
        self.list = []
        self.bd_cross_date = []
        self.pool = int(pool)
        self.ht = ht
        self.bd_id = bd_id
        self.start_dt = datetime.datetime.strptime(start_dt[0:8]+'01', '%Y-%m-%d')
        self.end_dt = datetime.datetime.strptime(end_dt[0:8]+'01', '%Y-%m-%d')
        for i in range(0, (self.end_dt - self.start_dt).days + 1):
            self.dt = (self.start_dt + datetime.timedelta(i)).strftime('%Y-%m-%d')
            if self.dt[8:10] == '01':
                self.list.append([self.dt,self.bd_id])
        self.last_year_period = list(map(lambda x : [str(int(x[0][0:4]) - 1) + x[0][4:10],x[1]],self.list))
        self.bd_cross_date = self.last_year_period + [item for item in self.list if item not in self.last_year_period]
        print ("当前年日期:\n")
        print (self.list)
        print ("同比日期:\n")
        print (self.bd_cross_date)


    def run(self):
        p = mp.Pool(self.pool)
        #p.map(bdcross, self.bd_cross_date)
        #p.map(lost,self.list)
        p.map(seq_job,self.list)




def main():
    ht = HiveTask()
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

    if len(sys.argv) <= 2:
        print ('Need to fill value of start_dt,end_dt,pool,bd_id(optional)...')
        raise AttributeError
    main_job = BEHV_MAIN(start_dt,end_dt, ht, pool,bd_id)
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
