#!/usr/bin/env python3
#!coding: utf-8
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
import multiprocessing as mp
from get_day_month import GET_MONTH_AND_DAY



class Super_Decorator(threading.Thread):
    def __init__(self):
        super(Super_Decorator, self).__init__()

    def __str__(self):
        return "Get Day list and Month List according to inputting parameters..."           #可在类实例化后的对象进行print, 内容即为return的值


    def __call__(self):
        return self.run()


    def run(self,*args,**kwargs):
        raise NotImplementedError('Subclass must implement \"run\" methond')

def Daily_deco(decorated_fc):
    class decorator(Super_Decorator):
        def run(self,*args,**kwargs):
            for i in GET_MONTH_AND_DAY().get_days():
                decorated_fc (i,GET_MONTH_AND_DAY().pool)
    return decorator()
def Month_deco(decorated_fc):
    class decorator(Super_Decorator):
        def run(self,*args,**kwargs):
            month_list = GET_MONTH_AND_DAY().get_month()
            decorated_fc(month_list,GET_MONTH_AND_DAY().pool)
    return decorator()
