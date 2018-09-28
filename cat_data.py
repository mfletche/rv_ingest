#!/usr/bin/env python
"""
Functions which catalogue the data on the routeview archive site. Can determine
if files exist, and find a list later than a given date, etc.

Author: Marianne Fletcher
"""

import datetime
import pytz
import re

baseUrl = 'http://archive.routeviews.org/route-views6/bgpdata/'

# Grouped into (year, month)
dirPattern = r'(20[01][\d])\.([01][\d])/'

# Grouped into (type, year, month, day, hour, minute)
filePattern = r'(rib|updates)\.(20[01][\d])([01]\d)([0-3]\d)\.([0-2]\d)([0134][05])\.bz2'

class RVData:
    """ Represents the routeview data on the archive website.
    """
    
    # Given the name of a subdirectory, work out what month and year
    # the data within it was recorded.
    def getMonth(self, dirname):
        matchObj = re.match(dirPattern, dirname, re.M|re.I)
        if matchObj:
            return (int(matchObj.group(1)), int(matchObj.group(2)))
    
    def getUTCTime(self, filename):
        """ Get the UTC time that a file was recorded by examining the filename.
        """
        matchObj = re.match(filePattern, filename, re.M|re.I)
        if matchObj:
            year = int(matchObj.group(2))
            month = int(matchObj.group(3))
            day = int(matchObj.group(4))
            hour = int(matchObj.group(5))
            minute = int(matchObj.group(6))
            
            tm = datetime.datetime(year, month, day, hour, minute)
            tm.tzinfo = pytz.timezone("UTC")
            
            return tm

def main():
    print RVData().getUTCTime('rib.20180901.0000.bz2')
    
main()