#coding=utf-8
import urllib2
import urllib
class html():
    def download_html(self,url):
        if url is None:
            return None
        try:
            response=urllib2.urlopen(url)
            if response.getcode() != 200:
                return None
            return response.read()
        except:
            pass
        #print response.read()
