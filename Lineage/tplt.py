#!/usr/bin/env python2
#!coding: utf-8
from jinja2 import Environment,FileSystemLoader
import os
import sys
import time
import re
if sys.getdefaultencoding() != 'utf-8':
    reload(sys)
    sys.setdefaultencoding('utf-8')
class Template_Maker:
    def __init__(self,para_dict,table_name=None):
        self.para_dict = para_dict
        self.table_name = table_name
    def Create_Template(self,template='template_1.py',filename=None):
        tplt_path=os.path.join('/home','mart_vdp','task','app_data','bin','tplt')
        env=Environment(loader=FileSystemLoader(tplt_path))
        template=env.get_template(template)
        out=template.render(self.para_dict)
        try:
            if filename is not None:
                # f = open('/home/mart_vdp/sql_template/%s' % filename, 'w')
                f = open(sys.argv[1]+'/%s' % filename, 'w')
                f.write(out)
                f.close()
            else:
                raise Exception
        except Exception as e:
            raise Exception("请输入文件名!")
            # f = open('/home/mart_vdp/sql_template/%s.py' % self.table_name,'w')
            # f.write(out)
            # f.close()

    @staticmethod
    def Sql_Input():
        sql_content = ''
        print("输入SQL:")
        while True:
            file_line = raw_input("sql line > ")
            if file_line == ':q':
                break
            else:
                sql_content += file_line + '\n'
        return sql_content

    @staticmethod
    def SqlAnalyze(sql_content):
        sql_content = re.sub(r'[\t]+', r'\n', sql_content)
        sql_content = re.sub(r'[\n]{2,}', r'\n', sql_content)
        sql_content = re.sub(r'from\s+', r'from\t', sql_content)
        parent_tabs = re.findall(r'from\s+(\b\w+\.{0,1}\w+?\b)', sql_content, re.I)
        for num,tbl in enumerate(parent_tabs):
            if tbl.startswith('app.'):
                parent_tabs[num] = tbl.split('.')[1]
        return sql_content, parent_tabs
if __name__ == '__main__':
    para_dict = {}
    #脚本创建时间
    create_time = time.strftime('%Y-%m-%d %H:%M:%S')
    para_dict['create_time'] = create_time
    #表名
    table_name = raw_input("输入表名:\n")
    para_dict['table_name'] = table_name
    #表comment
    comment = raw_input("输入表注释:\n")
    para_dict['comment'] = comment
    #分区
    partition = raw_input("输入表分区和类型(例dt string, bd_id string):\n")
    para_dict['partition'] = partition
    #脚本sql内容
    sql_content = Template_Maker.Sql_Input()
    sql_content, parent_tabs = Template_Maker.SqlAnalyze(sql_content)
    para_dict['sql_content'] = sql_content

    # 解析sql_content中的依赖

    # parent_tabs = re.findall(r'from\s+(\b\w+\.{0,1}\w+?\b)',sql_content,re.I)
    para_dict['dependent']={table_name:parent_tabs}


    #print (sql_content)
    class_obj = Template_Maker(para_dict,table_name=table_name)
    class_obj.Create_Template()
