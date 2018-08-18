#!/usr/bin/env python3
#!coding: utf-8
from subprocess import Popen,PIPE,STDOUT
import sys
import datetime
class DOWN_STREAM:
    def __init__(self):
        pass
    @staticmethod
    def run_job():
        encoding = 'utf-8'
        date = (datetime.datetime.strptime(sys.argv[4],'%Y-%m-%d')+datetime.timedelta(1)).strftime('%Y-%m-%d')#传入参数日期+1的日期,保证跑脚本时获取yesterday时与传入的参数一样
        cmd = 'python3 ./{0} {1}'.format(sys.argv[6],date)
        print ('开始执行脚本命令:')
        print (cmd)
        res = Popen(cmd.split(), stdout=PIPE, stderr=STDOUT)
        #res.wait()
        while True:
            line = res.stdout.readline().decode(encoding)
            if line == '' and res.poll() is not None:
                break
            print (line)
