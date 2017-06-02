#coding=utf-8
import os
import re
import urllib2
import urllib
import link_manager
import add_link
import download_html
import html_parser
from bs4 import BeautifulSoup
import write_excel
class main():
    def __init__(self):
        self.parser=html_parser.parser()
        self.download_html=download_html.html()
        self.link_manager=link_manager.links()
        self.add_link=add_link.add_link()
        self.write_excel=write_excel.write_excel()
    def crawl_main(self,link):
        path='E:\Study\Python\doubancrawl\crawl\pic'
        self.add_link.add_new_link(link)
        count=0
        num=1

        #if link=='https://book.douban.com/':
             #   html=self.download_html.download_html(link)
              #  self.parser.root_parser(path,html)
              #  new_links=self.link_manager.get_new_links(html)
              #  self.add_link.add_new_links(new_links)
        while self.add_link.has_new_url:
            new_url=self.add_link.get_new_url()
            if new_url=='https://book.douban.com/':
                html=self.download_html.download_html(new_url)
                self.parser.root_parser(path,html)
                new_links=self.link_manager.get_new_links(html)
                self.add_link.add_new_links(new_links)
            else:
                html=self.download_html.download_html(new_url)
                print "------------------------------",num,new_url
                pic_url_list=self.parser.get_new_pic_links(html)
                for link in pic_url_list:
                    urllib.urlretrieve(link,os.path.join(path,str(count)+'.jpg'))
                    count+=1
                new_links=self.link_manager.get_new_links(html)
                self.add_link.add_new_links(new_links)
                title,summary=self.parser.get_title_summary(html)
                writer,publisher,year,pages,price,style=self.parser.get_info(html)
                self.write_excel.write_in_title(num,title)
                self.write_excel.write_in_summary(num,summary)
                self.write_excel.write_in_publisher(num,publisher)
                self.write_excel.write_in_year(num,year)
                self.write_excel.write_in_pages(num,pages)
                self.write_excel.write_in_price(num,price)
                self.write_excel.write_in_style(num,style)
                self.write_excel.write_in_writer(num,writer)
                num+=1
            if num==100:
                break
a=main()
n=a.crawl_main('https://book.douban.com_test/')
