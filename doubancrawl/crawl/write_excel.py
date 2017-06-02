#coding=utf-8
import xlwt
from bs4 import BeautifulSoup
class write_excel():
    def __init__(self):
        self.book=xlwt.Workbook(encoding='utf-8',style_compression=0)
        self.sheet=self.book.add_sheet('info',cell_overwrite_ok=True)
        self.title_list=['名称','作者','出版社','出版年','页数','定价','装帧','简介']
        for i in range(0,8):
            self.sheet.write(0,i,self.title_list.pop(0))
        self.book.save('E:/Study/Python/doubancrawl/crawl/info.xls')

    def info_content(self,html):
        soup = BeautifulSoup(html,'html.parser',from_encoding='utf-8')

    def write_in_title(self,num,title):
        self.sheet.write(num,0,title)
        self.book.save('E:/Study/Python/doubancrawl/crawl/info.xls')
    def write_in_summary(self,num,summary):
        self.sheet.write(num,7,summary)
        self.book.save('E:/Study/Python/doubancrawl/crawl/info.xls')
    def write_in_publisher(self,num,publisher):
        self.sheet.write(num,2,publisher)
    def write_in_year(self,num,year):
        self.sheet.write(num,3,year)
    def write_in_pages(self,num,pages):
        self.sheet.write(num,4,pages)
    def write_in_price(self,num,price):
        self.sheet.write(num,5,price)
    def write_in_style(self,num,style):
        self.sheet.write(num,6,style)
    def write_in_writer(self,num,writer):
        self.sheet.write(num,1,writer)
