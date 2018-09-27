"""Represents an online directory on the routeview website. Can fetch the HTML
representing the directory and list files and folders.
"""

import pycurl
from StringIO import StringIO
from bs4 import BeautifulSoup

baseUrl = 'http://archive.routeviews.org/route-views6/bgpdata/'

# The alt text that will appear for links of different types
SUBDIRTYPE = '[DIR]'
FILETYPE = '[   ]'  # This works on routeview but if Apache knows the type of
                    # file there may be different alt-text


class OnlineDir:
    
    def __init__(self, url):
        self.url = url
        
    def update(self):
        """Re-fetch and re-parse the HTML for this directory.
        """
        self.body = None
        self.soup = None
        self.fetch()
        if self.body:
            self.parse()
            
    def fetch(self):
        """ Fetch the HTML describing the online directory.
        """
        # Use Curl to fetch resource
        buffer = StringIO()
        c = pycurl.Curl()
        c.setopt(c.URL, self.url)
        c.setopt(c.WRITEDATA, buffer)
        c.perform()
    
        # Check HTTP response code indicates OK
        if (c.getinfo(c.RESPONSE_CODE) != 200):
            print(str(c.RESPONSE_CODE) + ' error: Could not fetch ' + url)
            return# which contains an image with alt-text. This alt-text can be use
    
        c.close()
    
        self.body = buffer.getvalue()
        buffer.close()
        return self.body
    
    def parse(self):
        """ Parse HTML using BeautifulSoup parser.
        """
        if self.body:
            self.soup = BeautifulSoup(self.body, 'html.parser')
            return self.soup
    
    def listLinks(self):
        """ Returns a list of links on the online directory page, with
        their types.
        """
        list = []
        links = self.soup.find_all('a')
        for link in links:
            
            # Links to subfolders or files are in a table. A cell in the same
            # row as each link contains an icon with alt text which describes
            # whether the link is a directory, parent directory or file.
            if link.parent.parent.td:
                list.append((link['href'], link.parent.parent.td.img['alt']))
        return list
    
    def listSubdirs(self):
        """ Returns a list of subdirectories that appear on this online
        directory page.
        """
        linkList = self.listLinks()
        subdirList = []
        for link in linkList:
            url, type = link
            if type == SUBDIRTYPE:
                subdirList.append(url)
        return subdirList
    
    def listFiles(self):
        """ Returns a list of files that appear on this online directory page.
        """
        linkList = self.listLinks()
        fileList = []
        for link in linkList:
            url, type = link
            if type == FILETYPE:
                fileList.append(url)
        return fileList
    
    def getUrl(self, link):
        """ Constructs a link for retrieving the directory listing or a file.
        """
        # A leading forward slash denotes a non-relative link (not supported)
        if link[0]=='/':
            return
        return self.url + link