#!/usr/bin/env python3

from app_vdp_wric_user_behvr_cate_inner_new_v2 import UserBehCateInnerNew
from app_vdp_wric_user_behvr_cate_inner_new_rivals_day_v2 import UserBehCateInnerNewRivalsDay
from app_vdp_wric_user_behvr_cate_inner_new_sku_trend_day_v2 import UserBehCateInnerNewSkuTrendDay
from app_vdp_wric_user_behvr_cate_inner_new_user_trend_day_v2 import UserBehCateInnerNewUserTrendDay
#
from app_vdp_wric_user_behvr_cate_inner_new_rivals_month_v2 import UserBehCateInnerNewRivalsMonth
from app_vdp_wric_user_behvr_cate_inner_new_sku_trend_month_v2 import UserBehCateInnerNewSkuTrendMonth
from app_vdp_wric_user_behvr_cate_inner_new_user_trend_month_v2 import UserBehCateInnerNewUserTrendMonth
from app_vdp_wric_user_behvr_cate_inner_new_sku_trend_month_attr_v2 import UserBehCateInnerNewSkuTrendMonthAttr
from app_vdp_wric_user_behvr_cate_inner_new_sku_trend_month_gene_v2 import UserBehCateInnerNewSkuTrendMonthGene
from app_vdp_wric_user_behvr_cate_inner_new_sku_trend_month_rank_v2 import UserBehCateInnerNewSkuTrendMonthRank

import Decorator
from HiveTask import HiveTask
import multiprocessing as mp

def daily_job(date_list):
    ht = HiveTask()
    innernew = UserBehCateInnerNew(date_list[0], ht, date_list[1])
    innernew.run()
    rivals = UserBehCateInnerNewRivalsDay(date_list[0], ht, date_list[1])
    rivals.start()
    usertrend = UserBehCateInnerNewUserTrendDay(date_list[0], ht, date_list[1])
    usertrend.start()
    skutrend = UserBehCateInnerNewSkuTrendDay(date_list[0], ht, date_list[1])
    skutrend.start()
    rivals.join()
    usertrend.join()
    skutrend.join()
def monthly_job(date_list):
    ht = HiveTask()
    rivalsmth = UserBehCateInnerNewRivalsMonth(date_list[0], ht, date_list[1])
    rivalsmth.start()
    skutrendmth = UserBehCateInnerNewSkuTrendMonth(date_list[0], ht, date_list[1])
    skutrendmth.start()
    usertrendmth = UserBehCateInnerNewUserTrendMonth(date_list[0], ht, date_list[1])
    usertrendmth.start()
    rivalsmth.join()
    skutrendmth.join()
    usertrendmth.join()
    attr = UserBehCateInnerNewSkuTrendMonthAttr(date_list[0], ht, date_list[1])
    attr.start()
    gene = UserBehCateInnerNewSkuTrendMonthGene(date_list[0], ht, date_list[1])

    rank = UserBehCateInnerNewSkuTrendMonthRank(date_list[0], ht, date_list[1])
    rank.start()
    rank.join()
    attr.join()

    gene.start()
    gene.join()

#此处添加扩展脚本
# def 扩展脚本函数,格式如上
#每一个模块自己单独命名,daily和month分开写
