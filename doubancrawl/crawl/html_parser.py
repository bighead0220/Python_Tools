#coding=utf-8
import re
import urllib
import os
from bs4 import BeautifulSoup
#获取图片地址
class parser():
    #def __int__(self):
     #   self.soup = BeautifulSoup(html,'html.parser',from_encoding='utf-8')
    def get_new_pic_links(self,html):
        #soup = BeautifulSoup(html,'html.parser',from_encoding='utf-8')
        #https://book.douban.com/subject/26693234/
        link_list=[]
        #src="https://img3.doubanio.com/lpic/s28423186.jpg"
        pattern=re.compile(r'src="(https://.*\.jpg)')
        links=re.findall(pattern,html)
        for link in links:
            link_list.append(link)
        return link_list
    def save_pic(self,link_list):
        count=0
        path='E:\Study\Python\usualtest\pic'
        for link in link_list:
            urllib.urlretrieve(link,os.path.join(path,str(count)+'.jpg'))
            count+=1
        #return count
    def get_title_summary(self,html):
        summary=""
        soup = BeautifulSoup(html,'html.parser',from_encoding='utf-8')
        #print html

        summary_node=soup.find('div',class_="intro").find_all('p')
        #if type(summary_node) == 'None':
            #print "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            #print summary_node
            #summary_node_1=summary_node.find_all('p')
        for x in summary_node:
            summary=summary+x.string+'\n'
            #summary_list.append(summary.string)
            #print summary_list
        #res_data['summary']=summary_list
        title_node=soup.find('h1')
        title=title_node.get_text()
        return title,summary
        #res_data['title']=title
        #with open('test1.txt','w') as f:
        #    f.write(res_data['summary'])
        #return res_data
    #def get_price(self,html):
     #   soup = BeautifulSoup(html,'html.parser',from_encoding='utf-8')
     #   soup.find('div',id="info").find_all('br')
    def get_info(self,html):
            list=["出版社","出版年","页数","定价","装帧"]
            info_list_1=[]
            info_dict={"出版社":"","出版日期":"","页数":"","价格":"","装帧":""}
            info_dict={}
            soup = BeautifulSoup(html,'html.parser',from_encoding='utf-8')
            info=str(soup.find('div',id="info"))#.find_all('span',class_="p1")
            info_1=str(soup.find('div',id="info").find('span'))
            #print info
            pattern=re.compile(r'<span class="pl">(.+):</span>(.+)<br/>')

            writer_patter=re.compile(r'<a class="" href=".+?">(.+)</a>')
            info_list=re.findall(pattern,info)
            writer=re.findall(writer_patter,info_1)
            print writer
            #for x in info_list:
            info_list_1=[x[1] for x in info_list if x[0] in list]
            print info_list_1
            publisher=info_list_1[0]
            year=info_list_1[1]
            pages=info_list_1[2]
            price=filter(lambda ch:ch in '0123456789.',info_list_1[3])
            style=info_list_1[4]
            return writer,publisher,year,pages,price,style






    def root_parser(self,path,html):
        soup = BeautifulSoup(html,'html.parser',from_encoding='utf-8')
        cover=soup.find_all('div',class_="cover")
        data={"src":[],"title":[]}
        for x in cover:
            src=x.find('a').find('img')['src']
            title=x.find('a').find('img')['alt']
            title_1=re.sub(r'[:]',' ',title)
            data[src]=title_1
        for final in data:
            try:
                urllib.urlretrieve(final,os.path.join(path,data[final]+'.jpg'))
            except:
                print "wrong name %r"%data[final]