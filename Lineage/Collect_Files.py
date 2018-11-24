#!/usr/bin/env python2
#!coding: utf-8
import os
import ast
import sys
import re
class Collect_Files:
    def __init__(self,path):
        self.path = path
    def Collect(self): #收集某目录下非common子目录下的所有py文件,组成Pylist列表
        PyList = []
        root=os.walk(self.path)

        for (dir,cdir,files) in root:

            if  'common' in cdir:  #只找子目录为common时的根目录下的文件
                for pyfile in files:
                    if (pyfile.split('.')[1] == 'py' and pyfile.startswith('app')):
                        PyList.append(pyfile)
        return PyList
    def SqlAnalyze(self,pyfile,content):
        #紧跟在from后面的表
        content_lower = content.lower()
        child_from = re.findall(r'from\s+(\b\w+\.{0,1}\w+\b(?!\s+import))', content_lower, re.I)
        for num,tbl in enumerate(child_from):
            if tbl.startswith('app.'):
                child_from[num] = tbl.split('.')[1]
        #紧跟在join后面的表
        child_join = re.findall(r'.*join(?!\s+\()\s+(\w+)', content_lower, re.I)
        child_tabs = list(set(child_from+child_join))
        dependent = {pyfile.split('.')[0]:child_tabs}
        return dependent

    def Read_File(self,PyList):
        pydict={}
        for pyfile in PyList:
            file = open(os.path.join(self.path,pyfile),'r')
            content = file.read()
            dependent = self.SqlAnalyze(pyfile,content)
            pydict=dict(pydict,**dependent)
        return pydict
    # def Read_File(self,PyList):
    #     pydict={}
    #     for pyfile in PyList:
    #         file = open(os.path.join(self.path,pyfile),'r')
    #         for line in file:
    #             if line.startswith('Dependent'):
    #                 dependent = ast.literal_eval(line.split('-')[1])  #返回当前文件的依赖关系字典,返回的是字符串, 用ast.literal_eval转换为字典, 比用eval更安全
    #                 pydict=dict(pydict,**dependent)
    #     return pydict

    def Lineage(self,depdict):
        deplist=[]
        deplist1=[]
        for k, v in depdict.items():
            for n, i in enumerate(v):
                if i in depdict.keys():
                    depdict[k][n] = {depdict[k][n]: depdict[i]} #血缘关系整理成dict格式
        for k, v in depdict.items():
            element = "{" + "'" + k + "'" + ":" + str(v) + "}"  # 组成{key:value}形式的字符串
            deplist.append(element)
        for dep in deplist:
            dep_json = re.sub(r'{', '{\n\'text\':', dep)
            dep_json = re.sub(r':\s{0,1}\[', ',state:{\'opened\':true},\n\'children\':[', dep_json)
            dep_json = re.sub(r"\[\.{3,}\]", '[\'...\']', dep_json)
            deplist1.append(dep_json)
        JsonTree = (','.join(deplist1))
        return depdict,JsonTree

    def Json_Tree(self,JsonTree):
        JsonTree_dict = {"Json_Tree":JsonTree}
        tplt = __import__('tplt')
        template = getattr(tplt, 'Template_Maker')
        obj = template(JsonTree_dict)
        method = getattr(obj, 'Create_Template')
        res = method(template='Json_Tree.py',filename='index.html')
        return res

if __name__ == '__main__':
    path = sys.argv[1]
    class_obj = Collect_Files(path)
    pylist = class_obj.Collect()
    dependent = class_obj.Read_File(pylist)
    lineage,JsonTree = class_obj.Lineage(dependent)
    index_html = class_obj.Json_Tree(JsonTree)
    print ("任务完成,血缘关系下载 "+sys.argv[1]+"/index.html 查看")
