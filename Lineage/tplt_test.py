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
    def __init__(self,table_name=None):

        self.table_name = table_name
    def ReadSql(self,file):
        f = open(file, 'r')
        content = f.read()
        return content
    @staticmethod
    def SqlAnalyze(content):
        content_lower = content.lower()
        #紧跟在from后面的表

        child_from = re.findall(r'from\s+(\b\w+\.{0,1}\w+\b(?!\s+import))', content_lower, re.I)
        for num,tbl in enumerate(child_from):
            if tbl.startswith('app.'):
                child_from[num] = tbl.split('.')[1]
        #紧跟在join后面的表
        child_join = re.findall(r'.*join(?!\s+\()\s+(\w+)', content_lower, re.I)
        child_tabs = list(set(child_from+child_join))
        dependent = {sys.argv[1].split('.')[0]:child_tabs}
        print (dependent)
if __name__ == '__main__':
    class_obj = Template_Maker()
    content = class_obj.ReadSql(sys.argv[1])
    class_obj.SqlAnalyze(content)
