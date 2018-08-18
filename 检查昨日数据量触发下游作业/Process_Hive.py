#!/usr/bin/env python3
#!coding: utf-8
from subprocess import Popen,PIPE,STDOUT
import sys
class PROCESS_HIVE_CMD:
    def __init__(self):
        pass
    @staticmethod
    def get_location(desc,yesterday):
        encoding = 'utf-8'
        #location = ''

        res = Popen(desc,shell=True,stdout=PIPE, stderr=STDOUT)
        res.wait()
        while True:
            line = res.stdout.readline().decode(encoding)
            if line == '' and res.poll() is not None:
                break
            if line.find('Location') != -1:
                location = line.split()[1]
        try:
            print ('location值为: %s' % location)
        except AttributeError as e:
            print ('Location无值')
            raise AttributeError
        #hadoop_cmd = 'hadoop fs -count {0}/dt={1}'.format(location,yesterday)
        hadoop_cmd = 'hadoop fs -cat {0}/dt={1}/* | wc -l'.format(location, yesterday)
        #count_res = Popen(hadoop_cmd.split(),stdout=PIPE, stderr=STDOUT)
        count_res = Popen(hadoop_cmd, shell=True, stdout=PIPE, stderr=STDOUT)#因为命令中的 '|' 用split切分完不算是一个命令, 会报错, 所以把上面一行注释掉了, 用现在shell=True的方法
        count_res.wait()
        #cnt = count_res.communicate()[0].decode(encoding).split()[1]
        cnt = count_res.communicate()[0].decode(encoding)
        try:
            cnt = int(cnt)
            print("dt={0}的目录下的数据条数为:{1}".format(sys.argv[4], cnt))
            return int(cnt)
        except Exception as e:
            print("查询的分区dt={0}不存在!".format(sys.argv[4]))
            print(e)
            return cnt
    @staticmethod
    def get_select(select):
        encoding = 'utf-8'
        res = Popen(select, shell=True, stdout=PIPE, stderr=STDOUT)
        #res.wait()
        res_list = []
        while True:
            line = res.stdout.readline().decode(encoding)
            if line == '' and res.poll() is not None:
                break
            print (line)
            if line != '':
                res_list.append(line.strip())
        print ("iiiiiiiii")
        print (res_list)
        if res_list[-1].find('Time taken:') != -1:
            print ("uuuuuuu")
            print (res_list[-2])
            return int(res_list[-2])
        else:
            return 0


