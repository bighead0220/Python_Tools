#!/usr/bin/env python3
#!coding: utf-8

import sys
import time
import threading
import datetime
from datetime_util import DateTimeUtil
class GET_MONTH_AND_DAY:
    def __init__(self):
        # if len(sys.argv) == 5:
        #     self.jobDate_start = sys.argv[1]
        #     self.jobDate = sys.argv[2]
        #     print ('********\nself.jobDate_start\nself.jobDate\n********')
        #     self.pool = int(sys.argv[3])
        #     self.bd_id = sys.argv[4]
        # if len(sys.argv) == 4:
        #     self.jobDate_start = sys.argv[1]
        #     self.jobDate = sys.argv[2]
        #     self.pool = int(sys.argv[3])
        #     self.bd_id = None
        self.cur_date = datetime.date.today()
        self.jobDate = self.cur_date.strftime('%Y-%m-%d')
        self.jobDate_start = DateTimeUtil.oneday_by_date(self.jobDate[0:8] + '01', -1, '%Y-%m-%d')[0:8] + '01'
        self.pool = 5
        self.bd_id = None
        self.d1 = datetime.datetime.strptime(self.jobDate_start, '%Y-%m-%d')  # 第一个参数为跑数开始日期
        self.d2 = datetime.datetime.strptime(self.jobDate, '%Y-%m-%d')  # 第二个参数为跑数结束日期

    def get_days(self):  # 获取开始至结束日期的每日日期的列表,如 [['2017-02-01','PG'],['2017-02-02','PG'],['2017-02-03','PG']]
        # 判断起始日期与结束日期是否在同一年,若不在同一年,则其实日期调整至与结束日期同年的1月2号,因为如果跨年的话,一般日表也只需要当前年数据
        #if self.d1.year < self.d2.year:
        #    start_date = datetime.datetime(year=self.d2.year, month=1, day=2)
        #else:
        #    start_date = self.d1
        # i作为生成器每次返回10个结果时的边界,如1-10, 11-20, 21-30
        i = 0
        start_date = self.d1
        #end_date = self.d2
        print ((self.d2 - start_date).days)
        # 无限循环生成器
        while True:
            date_list_day = []  # 每次返回10个结果后,回到循环时置空list
            if i + 10 <= (self.d2 - start_date).days:  # 判断本次循环若返回10个结果,是否会超出结束日期,通过结束日期与开始日期之间相差的天数,来控制最后一次返回的结果,不会超出结束日期
                for x in range(i, i + 10):  # 若没超出结束日期,则顺序返回10个日期
                    date_list_day.append([(start_date + datetime.timedelta(x)).strftime('%Y-%m-%d'), self.bd_id])
                yield date_list_day  # 返回10个日期
                i += 10  # 下一次循环生成器时,将i+10
            else:  # 如果i+10超出了结束日期
                for x in range(i, (
                    self.d2 - start_date).days + 1):  # 则从当前的i值,到结束日期与开始日期的差值,比如结束日期减开始日期=28天,现在i=20,再加10就超过28了,所以就只取20至28这个范围,返回9个值,保证返回结果不会超出结束日期
                    date_list_day.append([(start_date + datetime.timedelta(x)).strftime('%Y-%m-%d'), self.bd_id])
                yield date_list_day  # 返回最后一次的日期结果
                raise StopIteration  # 生成器停止迭代


    def get_month(self):  # 获取开始日期与结束日期之间的所有月初日期, 这个不是生成器,因为月的日期较少,不适用生成器也可以不占用大量内存.
        date_list_month = []
        for i in range(0, (self.d2 - self.d1).days + 1):  # 直接循环开始日期至结束日期相差天数这么多次,如相差28天,则循环28次.
            if (self.d1 + datetime.timedelta(i)).day == 1:  # 如果循环到日等于1的日期时,放入列表
                date_list_month.append([(self.d1 + datetime.timedelta(i)).strftime('%Y-%m-%d'), self.bd_id])
        return date_list_month  # 返回列表
