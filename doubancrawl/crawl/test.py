#coding=utf-8
from bs4 import BeautifulSoup
import xlwt
'''
html='<div class="cover">' \
'<a href="https://read.douban.com/ebook/19373603/?dcs=book-hot&amp;dcm=douban&amp;dct=read-subject" target="_blank" title="IBM帝国缔造者 : 小沃森自传">' \
'<img src="https://img1.doubanio.com/view/ark_article_cover/cut/public/19373603.jpg?v=1457684543.0" alt="IBM帝国缔造者 : 小沃森自传" width="106px" height="158px">'\
'<img src="abc"'\
'</a>' \
'</div>'\
'<div class="cover">' \
'<a href="https://book.douban.com/subject/26652329/?icn=index-editionrecommend" title="失控">' \
'<img src="https://img3.doubanio.com/lpic/s28504961.jpg" class="" width="106px" height="158px" alt="失控">' \
'<img=123'
'</a>' \
'</div>'
soup = BeautifulSoup(html,'html.parser',from_encoding='utf-8')
a=soup.find_all('div',class_="cover")
list=[]
for x in a:
    n=x.find('img')['src']
    list.append(n)
print list
#list1=[]
#for x in a:
#    b=x.find('a').find('img')['alt']
#    print b
 #   list1.append(b)
'''

import write_excel
import download_html
'''
x=write_excel.write_excel()
def info_content(html):
        soup = BeautifulSoup(html,'html.parser',from_encoding='utf-8')
        info_nodes=soup.find('div',id="info").find_all('span')
        info_list=[]
        for info in info_nodes:
            info_list.append(info.string)
        print info_list

a=download_html.html()
html=a.download_html('https://book.douban.com/subject/26706379/?icn=index-editionrecommend')
info_content(html)
'''
'''
def get_summary_text(html):
        #res_data={"title":[],"summary":[]}
        #summary_list=[]
        str=""
        soup = BeautifulSoup(html,'html.parser',from_encoding='utf-8')
        summary_node=soup.find('div',class_="intro").find_all('p')
        for summary in summary_node:
            str=str+summary.get_text()+'\n'
            #res_data['summary'].append(summary.string)
            #summary_list.append(summary.string)
            #print summary.string
        #res_data['summary']=summary_list
        title_node=soup.find('h1')
        title=title_node.get_text()
        #res_data['summary']=summary_list
        #res_data['title']=title
        with open('text1,txt','w') as f:
            f.write(title+str)
        return title,str
'''
a=download_html.html()
html=a.download_html('https://book.douban.com/subject/26713431/?icn=index-editionrecommend')
list=[]
#x,y=get_summary_text(html)
import re
def get_publisher(html):
        list=["出版社","出版年","页数","定价","装帧"]
        info_list_1=[]
        info_dict={"出版社":"","出版日期":"","页数":"","价格":"","装帧":""}
        info_dict={}
        soup = BeautifulSoup(html,'html.parser',from_encoding='utf-8')
        info=str(soup.find('div',id="info"))#.find_all('span',class_="p1")
        print info
        pattern=re.compile(r'<span class="pl">(.+):</span>(.+)<br/>')
        info_list=re.findall(pattern,info)
        #print info_list
        #publisher=info_list.pop(0)
        pattern_1=str(soup.find('div',id="info").find('span'))
        print "---------------------"
        print pattern_1
        writer_patter=re.compile(r'<a class="" href=".+?">(.+)</a>')
        writer=re.findall(writer_patter,pattern_1)
        info_list_1=[x[1] for x in info_list if x[0] in list]
        print info_list_1
        print writer
        # count=0
       # for x in info_list[0:4]:
       #         info_dict[list[count]]=x
        #        count+=1
       # print info_dict
get_publisher(html)

'''
import write_excel
b=write_excel.write_excel()
num=1
b.write_in_title(num,x)
b.write_in_summary(num,y)
'''

