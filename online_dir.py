"""Represents an online directory on the routeview website. Can fetch the HTML
representing the directory and list files and folders.
"""

import pycurl
from StringIO import StringIO

baseUrl = 'http://archive.routeviews.org/route-views6/bgpdata/'

class OnlineDir:
    
    def __init__(self, url):
        self.url = url
            
    def fetch(self):
        """ Fetch the HTML describing the online directory.
        """
        # Use Curl to fetch resource
        buffer = StringIO()
        c = pycurl.Curl()
        c.setopt(c.URL, url)
        c.setopt(c.WRITEDATA, buffer)
        c.perform()
    
        # Check HTTP response code indicates OK
        if (c.getinfo(c.RESPONSE_CODE) != 200):
            print(str(c.RESPONSE_CODE) + ' error: Could not fetch ' + url)
            return
    
        c.close()
    
        self.body = buffer.getvalue()
        buffer.close()
        return body
    
def main():
    dir = OnlineDir(baseUrl)
    dir.fetch()
    print(dir.body)

if __name__ == "__main__":
    main()

