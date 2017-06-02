#coding=utf-8
import re
#获取链接地址
class links():
    def __init__(self):
        self.new_links=[]
        #self.all_links=[]
        #self.old_links=[]
    def get_new_links(self,html):
        #soup = BeautifulSoup(html,'html.parser',from_encoding='utf-8')
        #https://book.douban.com/subject/26693234/
        #link_list=[]
        #https://book.douban.com/subject/26733645/?icn=index-editionrecommend"
        pattern=re.compile(r'href="(https://book\.douban\.com/subject/\d{8}/\?icn=\S+?)"')
        links=re.findall(pattern,html)
        for link in links:
            self.new_links.append(link)
        #print self.new_links
        return self.new_links
