#!/usr/bin/env python3
"""
USAGE: 日期相关工具类
AUTHOR: liuxiaoze
MODIFIED BY: liuxiaoze
MODIFIED TIME: 2017年2月24日08:40:34
"""

import datetime
import time

class DateTimeUtil:

    @staticmethod
    def get_format_first_day(format):
        """
        获取本月第一天（字符串）
        """
        first_day = datetime.date(datetime.date.today().year, datetime.date.today().month, 1).strftime(format)
        return first_day

    @staticmethod
    def get_format_yesterday(format):
        """
        获取昨天日期（字符串）
        """
        yesterday = (datetime.date.today() - datetime.timedelta(1)).strftime(format)
        return yesterday

    @staticmethod
    def get_format_today(format):
        """
        获取今天日期（字符串）
        """
        today = datetime.date.today().strftime(format)
        return today


    @staticmethod
    def get_pre_month_begin(format):
        """
        获取上个月第一天（字符串）
        """
        pre_month_end = datetime.date(datetime.date.today().year, datetime.date.today().month, 1) - datetime.timedelta(1)
        begin_date = datetime.date(pre_month_end.year, pre_month_end.month, 1)
        return begin_date.strftime(format)

    @staticmethod
    def get_pre_month_end(format):
        """
        获取上个月最后一天（字符串）
        """
        pre_month_end = datetime.date(datetime.date.today().year, datetime.date.today().month, 1) - datetime.timedelta(1)
        return pre_month_end.strftime(format)

    @staticmethod
    def get_pre_week_begin(format):
        """
        获取上周第一天（字符串）
        """
        pre_week_begin = datetime.date.today() - datetime.timedelta(datetime.date.today().weekday()) - datetime.timedelta(7)
        return pre_week_begin.strftime(format)

    @staticmethod
    def get_pre_week_end(format):
        """
        获取上周最后一天（字符串）
        """
        pre_week_end = datetime.date.today() - datetime.timedelta(datetime.date.today().weekday()) - datetime.timedelta(1)
        return pre_week_end.strftime(format)

    @staticmethod
    def get_week_begin(count, format):
        """
        获取前N周第一天（字符串）
        """
        pre_week_begin = datetime.date.today() - datetime.timedelta(datetime.date.today().weekday()) - datetime.timedelta(count * 7)
        return pre_week_begin.strftime(format)

    @staticmethod
    def get_week_end(count, format):
        """
        获取前N周最后一天（字符串）
        """
        pre_week_end = datetime.date.today() - datetime.timedelta(datetime.date.today().weekday()) - datetime.timedelta((count - 1) * 7) - datetime.timedelta(1)
        return pre_week_end.strftime(format)

    @staticmethod
    def get_pre_week_number():
        """
        获取上周的周数
        """
        pre_week_end = datetime.date.today() - datetime.timedelta(datetime.date.today().weekday() + 1)
        return str(pre_week_end.isocalendar()[1])

    @staticmethod
    def get_pre_week_str():
        """
        获取上周的周数(如：2014-5 2014年第五周)
        """
        pre_week_end = datetime.date.today() - datetime.timedelta(datetime.date.today().weekday() + 1)
        return str(pre_week_end.year) + "-" + str(pre_week_end.isocalendar()[1])

    @staticmethod
    def get_week_begin_by_date(count, txDateStr, format):
        """
        获取指定日期前N周第一天（字符串）
        """
        txDate = datetime.datetime.fromtimestamp(time.mktime(time.strptime(txDateStr, format)))
        pre_week_begin = txDate - datetime.timedelta(txDate.weekday()) - datetime.timedelta(count * 7)
        return pre_week_begin.strftime(format)

    @staticmethod
    def get_week_end_by_date(count, txDateStr, format):
        """
        获取指定日期前N周最后一天（字符串）(格式相同）
        """
        txDate = datetime.datetime.fromtimestamp(time.mktime(time.strptime(txDateStr, format)))
        pre_week_end = txDate - datetime.timedelta(txDate.weekday()) - datetime.timedelta((count - 1) * 7) - datetime.timedelta(1)
        return pre_week_end.strftime(format)

    @staticmethod
    def get_year_by_date(count, txDateStr, format):
        txDate = datetime.datetime.fromtimestamp(time.mktime(time.strptime(txDateStr, format)))
        pre_week_end = txDate + datetime.timedelta(365*count)
        return pre_week_end.strftime('%Y')

    @staticmethod
    def get_week_end_by_date_format(count, txDateStr, fromFormat, toFormat):
        """
        获取指定日期前N周最后一天（字符串）(格式不同）
        """
        txDate = datetime.datetime.fromtimestamp(time.mktime(time.strptime(txDateStr, fromFormat)))
        pre_week_end = txDate - datetime.timedelta(txDate.weekday()) - datetime.timedelta((count - 1) * 7) - datetime.timedelta(1)
        return pre_week_end.strftime(toFormat)

    @staticmethod
    def get_pre_week_desc_by_date(txDateStr, format):
        """
        获取指定日期前1周的周数(如：2014-5 2014年第五周)
        """
        txDate = datetime.datetime.fromtimestamp(time.mktime(time.strptime(txDateStr, format)))
        pre_week_end = txDate - datetime.timedelta(txDate.weekday() + 1)
        return str(pre_week_end.year) + "-" + str(pre_week_end.isocalendar()[1])


    @staticmethod
    def oneday_by_date(txDateStr, offsize, format):
        """
        获取第N天（字符串）
        """
        txDate = datetime.datetime.fromtimestamp(time.mktime(time.strptime(txDateStr, format)))
        yesterday = (txDate + datetime.timedelta(offsize)).strftime(format)
        return yesterday

    @staticmethod
    def oneday_format_by_date(txDateStr, offsize, strFormat, toFormat):
        """
        获取第N天（字符串）
        """
        txDate = datetime.datetime.fromtimestamp(time.mktime(time.strptime(txDateStr, strFormat)))
        yesterday = (txDate + datetime.timedelta(offsize)).strftime(toFormat)
        return yesterday

def main():
    d = DateTimeUtil.get_format_today('%Y-%m-%d')
    print('今天：' + d)

    d = DateTimeUtil.get_format_yesterday('%Y-%m-%d')
    print('昨天：' + d)

    d = DateTimeUtil.get_pre_month_begin('%Y-%m-%d')
    print('上个月第一天：' + d)

    d = DateTimeUtil.get_pre_month_end('%Y-%m-%d')
    print('上个月最后一天：' + d)

    d = DateTimeUtil.get_format_first_day('%Y-%m-%d')
    print('本月第一天：' + d)

    d = DateTimeUtil.get_pre_week_begin('%Y-%m-%d')
    print('上周第一天：' + d)

    d = DateTimeUtil.get_pre_week_end('%Y-%m-%d')
    print('上周最后一天：' + d)

    week_number = DateTimeUtil.get_pre_week_number()
    print('上周周数：' + week_number)

    week_str = DateTimeUtil.get_pre_week_str()
    print('上周周数：' + week_str)

    print('前5周第一天：' + DateTimeUtil.get_week_begin(5, '%Y-%m-%d'))
    print('前5周最后一天：' + DateTimeUtil.get_week_end(5, '%Y-%m-%d'))

    print('指定日期: 2014-02-01')
    print('指定日期前5周第一天：' + DateTimeUtil.get_week_begin_by_date(5, '2014-02-01', '%Y-%m-%d'))
    print('指定日期前5周最后一天：' + DateTimeUtil.get_week_end_by_date(5, '2014-02-01', '%Y-%m-%d'))

    print('指定日期前1周第一天：' + DateTimeUtil.get_week_begin_by_date(1, '2014-02-01', '%Y-%m-%d'))
    print('指定日期前1周最后一天：' + DateTimeUtil.get_week_end_by_date(1, '2014-02-01', '%Y-%m-%d'))

    print('指定日期前1周的周数：' + DateTimeUtil.get_pre_week_desc_by_date('2014-02-01', '%Y-%m-%d'))

    print('指定日期第N天：' + DateTimeUtil.oneday_by_date('2014-02-01', -1, '%Y-%m-%d'))

    print('本周第一天：' + DateTimeUtil.get_week_begin(0, '%Y-%m-%d'))

if __name__ == '__main__':
    main()


