#!/usr/bin/env python3
#coding: utf-8
#相比老版可以带出每个表的格式
import sys
import multiprocessing as mp
import subprocess
import re
import os
import time
file_path = '/home/mart_vdp/lxztest/app_file_count/'
def get_dir(table):
    get_dir = """hive -e 'use """ + database + """; desc formatted """
    get_dir_cmd = "%s%s;'" % (get_dir, table)
    table_dir = subprocess.Popen(get_dir_cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    content = ','.join('^'+x.decode('utf-8').strip()+'^' for x in table_dir.stdout.readlines())
    m = re.match(r'.*\^(Location.*?)\^.*\^(InputFormat.*?)\^', content)
    print ("Processing: %s" % table)
    if m is None:
        return table,[None,None]
    else:
        print (m.group(1).split()[1])
        print (m.group(2).split()[1])
        location = m.group(1).split()[1]
        format = m.group(2).split()[1]
        return table,[location,format]        #此处得到词典,{表名:(路径,表格式)}



def count_cmd(location_tuple):  #传入list,[('table_name', ('location', 'format'))]
    count_files = """hadoop fs -count """
    table_name = location_tuple[0]    #分解list, 得到表名
    location = location_tuple[1][0]   #得到location
    format = location_tuple[1][1]     #得到format
    count_cmd = "%s%s" % (count_files, location)
    print ("Executing: %s" % count_cmd)
    try:
        count_process = subprocess.Popen(count_cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        #print (count_process.stdout.readline().decode('utf-8').strip())  #这里不能print,因为print的内容是读取stdout,这里读取完之后,下一行再读取stdout就是空了,再split()[1]就会报错超出范围
        file_number = count_process.stdout.readline().decode('utf-8').strip().split()[1]
        result = "%s^%s^%s^%s\n" % (table_name, file_number, location, format)
        return result
    except Exception as e:
        print ("Error:%s,%s"%(table_name,e))
        return "%s^%s^%s^%s\n"%(table_name,None,None,None)


def count_small_files(location_tuple):  #传入list,[('table_name', ('location', 'format'))]
    count_small = 0  #小文件计数器
    count_file = 0   #文件计数器
    count_small_files = """hadoop fs -ls """
    table_name = location_tuple[0]    #分解list, 得到表名
    location = location_tuple[1][0]   #得到location
    format = location_tuple[1][1]     #得到format
    count_small_cmd = "%s%s/*" % (count_small_files, location)  #命令为hadoop fs -ls 路径/* 展示该路径下所有分区下的所有文件
    print (count_small_cmd)
    try:
        count_small_process = subprocess.Popen(count_small_cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        while count_small_process.poll() is None:
            file_detail = count_small_process.stdout.readline().decode('utf-8').strip()  #获取命令的每一行输出
            if file_detail.startswith('Found') or file_detail == '':
                continue
            else:
                count_file += 1
                file_size = int(file_detail.split()[4])
                if file_size < 104857600:   #100M = 104857600 b  128M = 134217728 b
                    count_small += 1
        result = "%s^%s^%s^%s^%s\n" % (table_name, count_file, count_small, location,format)
        return result
    except Exception as e:
        print ("Error:%s,%s"%(table_name,e))
        return "%s^%s^%s^%s^%s\n"%(table_name,None,None,None,None)



class CHECK_HADOOP_FILE_NUMBERS:
    def __init__(self, database):
        self.database = database
        #self.get_tables = """hive -e \"use app; show tables like 'app_vdp_cx_all*';\""""
        self.get_tables = """hive -e \"use """ + self.database + """; show tables;\""""
        #self.get_dir_cmd = """hive -e 'use app; desc formatted """
        self.decode = 'utf-8'
        self.pool = 30   #控制进程池内的进程数量
    def Path_check(self,path):
        if os.path.exists(path) == False:
            os.system('mkdir -p %s' % path)

    def get_tables_list(self):
        tables = []
        p_tables = subprocess.Popen(self.get_tables, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        while p_tables.poll() is None:
            table = p_tables.stdout.readline().decode(self.decode).strip()
            if 'FAILED' in table:
                print (table)
                break
            if re.match(r'.*\s.*|^OK$|^$', table):
                continue
            else:
                tables.append(table)
        return tables

    def get_table_location(self,table_list):
        p = mp.Pool(self.pool)
        #multiprocess.pool是进程池,用来控制多少个进程可以同时并发
        # 用pool之后,使用map,
        # 相当于multiprocess的Process(target=get_dir,args=(table这个table是遍历table_list的每一个元素,)),等价于当前pool里用到的'n'
        # 最终结果产生一个每一个参数传入get_dir后返回的结果组成的list
        # 比multiprocess好像方便点
        # 注意:用pool的时候要把get_dir这个位置的函数写在整个类的外面,因为pool只能接受一个传参,所以不能带self,否则报pickleError错误
        desc_location_list = p.map(get_dir, [n for n in table_list])  #是否能用生成器?
        location_dict = dict(desc_location_list)
        return location_dict  #此处得到词典,{表名:(路径,表格式)}

    def dict_to_list(self,location_dict):
        location_list=[]
        for key,value in location_dict.items():
            location_list.append((key,value))
        return location_list   #将词典元素以tuple形式组成list,返回list,[('table_name', ('location', 'format'))]

    def count_files(self,location_list):
        count = mp.Pool(self.pool)
        #result_list = count.map(count_cmd,location_list)
        result_list = count.map(count_small_files, location_list)
        return result_list
    def run_job_seq(self):
        table_list = self.get_tables_list()
        print ("Now received all %s tables" % len(table_list))
        location_dict = self.get_table_location(table_list)
        location_list = self.dict_to_list(location_dict)
        # print ("&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
        # print (location_list)
        result_list = self.count_files(location_list)
        self.Path_check(file_path)
        f = open("%sapp_result_%s.txt"%(file_path,self.database), 'w+')
        f.write("""table_name^count_file^count_small^location^format\n""")
        for line in result_list:
            f.write(line)
        f.close()

    def run(self):
        self.run_job_seq()
if __name__ == '__main__':
    #database = sys.argv[1]
    #for database in ['default','dev','dim','fdm','gdm','jdata','mart_dim','temp','test','tmp']:
    for database in ['app']:
        main_job = CHECK_HADOOP_FILE_NUMBERS(database)
        main_job.run()
    print ("All DONE")


