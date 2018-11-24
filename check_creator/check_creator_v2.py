#!/usr/bin/env python3
import subprocess
import re
import time
class CHECK_CREATOR:
    def __init__(self,table_list):
        self.load_tables = """hive -e "use app; show tables";"""
        #self.load_tables = """hive -e "use app; show tables like 'app_vdp_wric_user_sales_portrait_ration*'";"""
        self.decode = 'utf-8'
        self.result=[]
        self.creator_dict={}
        self.table_list = table_list.split(',')
        #print (self.load_tables)
    def run(self):
        # p = subprocess.Popen(self.load_tables, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        # while True:
        #     result_dict = {}
        #     table = p.stdout.readline().decode(self.decode).strip()
        #     print ("Now processing: %s"%table)
        #     if re.match(r'.*\s.*|^OK$|^$',table):
        #         print ("pass pass pass pass pass")
        #         if p.poll() is not None and table == '':
        #             break
        #         continue

        for i in self.table_list:
            result_dict = {}
            process_cmd = "hive -e 'use app; desc formatted %s;'" % i
        #process_cmd = self.process(table)
            process_tbl= subprocess.Popen(process_cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
            tbl_info = process_tbl.stdout.readlines()
            for content in tbl_info:
                content_list = content.decode(self.decode).strip().split()
                if len(content_list) > 0:
                    #self.result_dict = {}
                    #print (content_list)
                    result_dict[content_list[0]] = content_list[1:]
                    #print ("Table %s doesn't have a creator!" % table)
        #print (result_dict)
        #for key in result_dict:
        #    print (key,result_dict[key])
            if 'creator' not in result_dict.keys():
                self.result.append(i)
            else:
                self.creator_dict[i]=result_dict['creator']
        #print (self.creator_dict)
        print ("All %s tables have been checked! There are %s tables don't have creator" %(len(self.table_list),len(self.result)))
        write_file = open('/home/mart_vdp/lxztest/test/table_result.txt','w')
        creator_file = open('/home/mart_vdp/lxztest/test/table_creator.txt','w')
        for i in self.result:
            write_file.write(str(i) +'\n')
        write_file.close()
        for key in self.creator_dict:
            creator = "%s : %s" % (key,''.join(self.creator_dict[key]))
            creator_file.write(creator +'\n')
        creator_file.close()

    # def process(self,table):
    #     process_cmd = "hive -e 'use app; desc formatted %s;'" % table
    #     print ("***********%s"%process_cmd)
    #     return process_cmd


def main():
    import sys
    table_list = sys.argv[1]
    check_creator = CHECK_CREATOR(table_list)
    check_creator.run()

if __name__ == '__main__':
    stime = time.time()
    main()
    etime = time.time()
    diff = (etime - stime) / 60
    print ("Time: %sminutes" % diff)