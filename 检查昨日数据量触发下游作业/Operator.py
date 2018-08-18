#!/usr/bin/env python3
#!coding: utf-8
from Hive_cmd import Hive_Cmd
import subprocess
import sys
import time
from Counting_down import COUNTING
from Down_stream import DOWN_STREAM
class ExecError(Exception):
    def __init__(self):
        Exception.__init__(self)
    def __str__(self):
        return '超过设定执行次数'
class Operator:
    def __init__(self,operator,database,tablename,date,**kwargs):
        self.operator = operator
        self.database = database
        self.tablename = tablename
        self.date = date
        self.encoding = 'utf-8'
        self.try_times = int(kwargs['try_times']) if 'try_times' in kwargs.keys() else 0
    def execerror(self):
        raise ExecError()
    def check(self):
        hive_cmd_instance = Hive_Cmd(self.operator,self.database,self.tablename,self.date)
        hive_cmd_str = hive_cmd_instance()
        if self.operator == 'desc':
            num = 1
            # hive_cmd_instance调用Process_Hive里的get_location函数,得到表location下的文件个数,返回的是一个
            #数字,所以判断hive_cmd_str是否为整型就可以知道是否返回了文件个数.如果路径下为空,返回的不是数字而是字符串
            while not isinstance(hive_cmd_str,int) and num != self.try_times:
                print ("第{0}次重试...".format(num))
                COUNTING.counting_down()
                hive_cmd_instance = Hive_Cmd(self.operator, self.database, self.tablename, self.date)
                hive_cmd_str = hive_cmd_instance()
                num += 1

            if not isinstance(hive_cmd_str,int):
                self.execerror()
            elif hive_cmd_str > 2: #lzo格式的空文件是两行, orc的是0行, 无法确认文件格式的话, 无法通过文件行数判断是否为空文件
                print('分区dt={0}文件数量获取成功'.format(self.date))
                print('开始触发下游脚本...')
                DOWN_STREAM.run_job()
                print ("脚本执行完毕")
            else:
                print ("数据为空,抛出异常")
                print('执行{0}次后没有获得上游数据,停止脚本'.format(self.try_times))
                raise ExecError()
            # except ExecError as e:   #其他脚本内的except在这里不抛出,本脚本内的try失败后会进入这里的except
            #     print(e)







        if self.operator == 'select':
            num = 1
            while hive_cmd_str <= 0 and num != self.try_times:
                print("第{0}次重试...".format(num))
                COUNTING.counting_down()
                hive_cmd_instance = Hive_Cmd(self.operator, self.database, self.tablename, self.date)
                hive_cmd_str = hive_cmd_instance()
                num += 1
            if hive_cmd_str <= 0:
                self.execerror()
            else:
                print("dt={0}的数据量大于0".format(sys.argv[4]))
                print('开始触发下游脚本...')
                try:
                    DOWN_STREAM.run_job()
                    print("脚本执行完毕")
                except Exception as e:  # 不走except,查原因
                    print(e)


if __name__=='__main__':
    if sys.argv[1] == '-h':
        print ("参数用法:\n1.查询方法(select/desc)\n2.库名(app等)\n3.表名\n4.查询日期(如2017-12-31)\n5.重试等待间隔(分钟)\n6.下游脚本名称(带.py后缀)\n7.上游脚本尝试次数")
    else:
        operator = sys.argv[1]
        database = sys.argv[2]
        tablename = sys.argv[3]
        date = sys.argv[4]
        minute = sys.argv[5]
        downstream = sys.argv[6]
        if len(sys.argv) > 7:
            try_times = sys.argv[7]
            a = Operator(operator=operator,database=database,tablename=tablename,date=date,minute=minute,try_times=try_times)
            a.check()
        else:
            #a = Operator(operator=operator, database=database, tablename=tablename,minute=minute,date=date)
            print('使用./Operator.py -h查看传参规则')


