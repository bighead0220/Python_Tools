#coding=utf-8
class add_link():
    def __init__(self):
        self.new_url_list=[]
        self.old_url_list=[]
    def add_new_link(self,link):
        if link not in self.new_url_list and link not in self.old_url_list:
            self.new_url_list.append(link)
        #else:
            #print "url existed"
        return self.new_url_list
    def add_new_links(self,links):
        if links is None:
            return None
        else:
            for link in links:
                self.add_new_link(link)
            return self.new_url_list
    def has_new_url(self):
        if len(self.new_url_list) != 0:
            return True
    def get_new_url(self):
        new_url=self.new_url_list.pop()
        self.old_url_list.append(new_url)
        return new_url