#!/usr/bin/env python3
#!coding: utf-8
import datetime
from Process_Hive import PROCESS_HIVE_CMD
class CmdException(Exception):
    def __init__(self):
        pass
    def __str__(self):
        return 'HIVE查询命令不正确!(可用命令select,desc)'
class Hive_Cmd:
    def __init__(self,operator,database,table,date,**kwargs):
        self.operator = operator
        self.database = database
        self.table = table
        self.date = date
        self.today = datetime.datetime.today()
        self.yesterday = (self.today+datetime.timedelta(-1)).strftime('%Y-%m-%d')
    def __call__(self):
        return self.run()
    def __desc(self):
        print ("开始执行...\n当前执行命令:")
        cmd = 'hive -e \"use {0}; desc formatted {0}.{1};\"'.format(self.database,self.table)
        print (cmd)
        return cmd
    def __select(self):
        print("当前执行命令:")
        cmd = 'hive -e \"select count(1) from {0}.{1} where dt=\'{2}\'\"'.format(self.database,self.table,self.date)
        print (cmd)
        return cmd
    def run(self):
        if self.operator == 'desc':
            desc = self.__desc()
            cnt = PROCESS_HIVE_CMD.get_location(desc,self.date)
            return cnt

        elif self.operator == 'select':
            select = self.__select()
            entries_num = PROCESS_HIVE_CMD.get_select(select)
            return entries_num
        else:
            raise CmdException