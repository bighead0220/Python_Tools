#!/usr/bin/env python3

from app_vdp_wric_user_behvr_cate_inner_new_v2 import UserBehCateInnerNew
from app_vdp_wric_user_behvr_cate_inner_new_rivals_day_v2 import UserBehCateInnerNewRivalsDay
from app_vdp_wric_user_behvr_cate_inner_new_sku_trend_day_v2 import UserBehCateInnerNewSkuTrendDay
from app_vdp_wric_user_behvr_cate_inner_new_user_trend_day_v2 import UserBehCateInnerNewUserTrendDay
# #from app_vdp_wric_user_behvr_cate_inner_new_sku_trend_day_rtsku_v2 import UserBehCateInnerNewSkuTrendDayRtsku
#
from app_vdp_wric_user_behvr_cate_inner_new_rivals_month_v2 import UserBehCateInnerNewRivalsMonth
from app_vdp_wric_user_behvr_cate_inner_new_sku_trend_month_v2 import UserBehCateInnerNewSkuTrendMonth
from app_vdp_wric_user_behvr_cate_inner_new_user_trend_month_v2 import UserBehCateInnerNewUserTrendMonth
from app_vdp_wric_user_behvr_cate_inner_new_sku_trend_month_attr_v2 import UserBehCateInnerNewSkuTrendMonthAttr
from app_vdp_wric_user_behvr_cate_inner_new_sku_trend_month_gene_v2 import UserBehCateInnerNewSkuTrendMonthGene
from app_vdp_wric_user_behvr_cate_inner_new_sku_trend_month_rank_v2 import UserBehCateInnerNewSkuTrendMonthRank

#------20180621添加全球购模块脚本-------------
from app_vdp_wric_qqg_user_behvr_cate_inner_new_v2 import QQGUserBehCateInnerNew
from app_vdp_wric_qqg_user_behvr_cate_inner_new_rivals_day_v2 import QQGUserBehCateInnerNewRivalsDay
from app_vdp_wric_qqg_user_behvr_cate_inner_new_sku_trend_day_v2 import QQGUserBehCateInnerNewSkuTrendDay
from app_vdp_wric_qqg_user_behvr_cate_inner_new_user_trend_day_v2 import QQGUserBehCateInnerNewUserTrendDay
from app_vdp_wric_qqg_user_behvr_cate_inner_new_rivals_month_v2 import QQGUserBehCateInnerNewRivalsMonth
from app_vdp_wric_qqg_user_behvr_cate_inner_new_sku_trend_month_v2 import QQGUserBehCateInnerNewSkuTrendMonth
from app_vdp_wric_qqg_user_behvr_cate_inner_new_user_trend_month_v2 import QQGUserBehCateInnerNewUserTrendMonth
from app_vdp_wric_qqg_user_behvr_cate_inner_new_sku_trend_month_attr_v2 import QQGUserBehCateInnerNewSkuTrendMonthAttr
from app_vdp_wric_qqg_user_behvr_cate_inner_new_sku_trend_month_gene_v2 import QQGUserBehCateInnerNewSkuTrendMonthGene
from app_vdp_wric_qqg_user_behvr_cate_inner_new_sku_trend_month_rank_v2 import QQGUserBehCateInnerNewSkuTrendMonthRank
#------20180621添加结束-----------------

import Decorator
from HiveTask import HiveTask
import multiprocessing as mp

def daily_job(date_list):
    ht = HiveTask()
    qqginnernew = QQGUserBehCateInnerNew(date_list[0], ht, date_list[1])  #20180621 添加全球购

    qqginnernew.start()																										#20180621 添加全球购
    qqginnernew.join()																										#20180621 添加全球购
    qqgrivals = QQGUserBehCateInnerNewRivalsDay(date_list[0], ht, date_list[1]) #20180621 添加全球购
    qqgrivals.start()																														#20180621 添加全球购
    qqgusertrend = QQGUserBehCateInnerNewUserTrendDay(date_list[0], ht, date_list[1]) #20180621 添加全球购
    qqgusertrend.start()																												#20180621 添加全球购
    qqgskutrend = QQGUserBehCateInnerNewSkuTrendDay(date_list[0], ht, date_list[1]) #20180621 添加全球购
    qqgskutrend.start()																										#20180621 添加全球购
    qqgrivals.join()
    qqgusertrend.join()
    qqgskutrend.join()
def monthly_job(date_list):
    ht = HiveTask()
    qqgrivalsmth = QQGUserBehCateInnerNewRivalsMonth(date_list[0], ht, date_list[1])	#20180621 添加全球购
    qqgrivalsmth.start()																															#20180621 添加全球购
    qqgskutrendmth = QQGUserBehCateInnerNewSkuTrendMonth(date_list[0], ht, date_list[1]) #20180621 添加全球购
    qqgskutrendmth.start()																													  #20180621 添加全球购
    qqgusertrendmth = QQGUserBehCateInnerNewUserTrendMonth(date_list[0], ht, date_list[1])  #20180621 添加全球购
    qqgusertrendmth.start()																														#20180621 添加全球购
    qqgrivalsmth.join()																																#20180621 添加全球购
    qqgskutrendmth.join()																															#20180621 添加全球购
    qqgusertrendmth.join()																														#20180621 添加全球购
    qqgattr = QQGUserBehCateInnerNewSkuTrendMonthAttr(date_list[0], ht, date_list[1])	#20180621 添加全球购
    qqgattr.start()																																		#20180621 添加全球购
    qqggene = QQGUserBehCateInnerNewSkuTrendMonthGene(date_list[0], ht, date_list[1]) #20180621 添加全球购
    qqgrank = QQGUserBehCateInnerNewSkuTrendMonthRank(date_list[0], ht, date_list[1]) #20180621 添加全球购
    qqgrank.start()																																		#20180621 添加全球购
    qqgrank.join()																																		#20180621 添加全球购
    qqgattr.join()																																		#20180621 添加全球购
    qqggene.start()																																		#20180621 添加全球购
    qqggene.join()																																		#20180621 添加全球购

#此处添加扩展脚本
# def 扩展脚本函数,格式如上
#每一个模块自己单独命名,daily和month分开写
