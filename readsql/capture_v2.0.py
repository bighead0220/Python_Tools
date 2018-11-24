#!/usr/bin/env python3
#coding:utf-8
import subprocess
import sys
import time
import os
import parameter
#---------------------------------------
#Author: liuxiaoze
#Created time: 2017年10月27日14:51:34
#用法: ./capture.py 根据前两个提示输入erp和目标文件夹名, 例如 liuxiaoze,cx_all
#根据第三个提示复制sql语句到命令行,支持输入回车,以:q结尾作为sql语句结束标志
#根据前两个输入的erp和文件夹名创建目录/home/mart_vdp/task/app_data/bin/script/liuxiaoze/cx_all/
#并在该目录下生成两个文件:
# 1. cx_all_result.txt为sql执行结果,自行下载到windows下插入到excel中即可.
# 2. cx_all.sql,即为第三个提示下输入的sql内容.

class CaptureInput:
    def __init__(self,erp,sql_name,sql_content,delimiter='^'):
        self.create_time = time.strftime("%Y_%m_%d_%H_%M_%S",time.localtime())
        self.delimiter = delimiter
        self.decode = 'utf-8'
        self.sql_query = sql_content
        self.sql_name = sql_name  #sql文件名
        self.erp = erp
        self.sql_result = "%s_result_%s" % (self.sql_name,self.create_time)  #sql运行结果文件目录
        self.path_prefix = '/home/mart_vdp/task/app_data/bin/script'
        self.dest_path = "%s/%s/%s" % (self.path_prefix,self.erp,self.sql_name) #/home/mart_vdp/task/app_data/bin/script/liuxiaoze/cx_all
        self.sql_result_file = "%s/%s" % (self.dest_path, self.sql_result)  # /home/mart_vdp/task/app_data/bin/script/liuxiaoze/cx_all/cx_all_result.txt
        self.sql_file = "%s/%s_%s.sql" % (self.dest_path, self.sql_name,self.create_time)  # 执行的sql语句文件/home/mart_vdp/task/app_data/bin/script/liuxiaoze/cx_all/cx_all.sql
        self.para=parameter.PARAMATER.get_para()
        self.hive_cmd = """hive -e \"%s\ninsert overwrite local directory\t'%s'\nrow format delimited\nfields terminated by '%s'\n%s\"""" % (self.para,self.sql_result_file, self.delimiter,self.sql_query)
        print ("当前时间: %s" % self.create_time)
        print ("当前sql: %s" %self.hive_cmd)

    def Path_check(self):
        if os.path.exists(self.dest_path) == False:
            os.system('mkdir -p %s' % self.dest_path)

    def Create_file(self):
        sql_file = open(self.sql_file,'w')
        sql_file.write(self.sql_query)
        sql_file.close()

    def Capture_body(self):
        flag = 0
        cmd = subprocess.Popen(self.hive_cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        while cmd.poll() is None:
            line = cmd.stdout.readline().decode(self.decode)
            print (line)
            if 'FAILED' in line:
                os.system("rm %s" % (self.sql_result_file))
                flag = 1
        if flag == 0:
            self.Create_file()
            print ("Files have created as below:\nResult location: %s\nSQL file location: %s" % (self.sql_result_file,self.sql_file))

    def run(self):
        self.Path_check()
        self.Capture_body()

def main():
    erp = input("Please input erp:")
    sql_name = input("Please input project name:")
    delimiter = input("Please input delimiter:")
    sql_content=''
    print ("Please enter sql:")
    while True:     #此段while循环用于解决读取带回车的多行手动输入. 默认的input遇到回车就结束,这里逻辑等于把回车结束改为:q结束
        file_line = input("sql line > ")
        if file_line == ':q':
            break
        else:
            sql_content += file_line + '\n'
    main_job = CaptureInput(erp, sql_name,sql_content,delimiter)
    main_job.run()

if __name__ == '__main__':
    main()