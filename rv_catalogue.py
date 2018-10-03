#!/usr/bin/env python
"""
Functions which catalogue the data on the routeview archive site. Can determine
if files exist, and find a list later than a given date, etc.

Author: Marianne Fletcher
"""

import arrow
import pytz
import re
from online_dir import OnlineDir

baseUrl = 'http://archive.routeviews.org/route-views6/bgpdata/'

# Grouped into (year, month)
dirPattern = r'(20[01][\d])\.([01][\d])/'

# Grouped into (type, year, month, day, hour, minute)
filePattern = r'(rib|updates)\.(20[01][\d])([01]\d)([0-3]\d)\.([0-2]\d)([0134][05])\.bz2'
    
class RVCatalogue:
    """ Provides functions which will interpret the file and folder names in
    the routeview archive. Can extract a list of all files created at or after
    a provided UTC time.
    """
    
    # Given the name of a subdirectory, work out what month and year
    # the data within it was recorded.
    @staticmethod
    def getMonth(dirname):
        matchObj = re.match(dirPattern, dirname, re.M|re.I)
        if matchObj:
            year = int(matchObj.group(1))
            month = int(matchObj.group(2))
            
            tm = arrow.get(year, month, 1)
            return tm
    @staticmethod
    def getUTCTime(filename):
        """ Get the UTC time that a file was recorded by examining the filename.
        """
        matchObj = re.match(filePattern, filename, re.M|re.I)
        if matchObj:
            year = int(matchObj.group(2))
            month = int(matchObj.group(3))
            day = int(matchObj.group(4))
            hour = int(matchObj.group(5))
            minute = int(matchObj.group(6))
            
            tm = arrow.get(year, month, day, hour, minute)
            return tm

    @staticmethod
    def listDataAfter(dir, tm):
        """ Finds files which the filenames indicate were created at or after
        tm. Recurses through subdirectories.
        
        tm - Must be UTC
        """
        list = []
        dir = OnlineDir(dir)
        subdirs = dir.listSubdirs()
        for subdir in subdirs:
            # If this directory contains RIB and UPDATES folders getMonth will
            # return None, otherwise the name of the folder will give us the
            # month and year.
            if (RVCatalogue.getMonth(subdir) == None) or (RVCatalogue.getMonth(subdir) >= tm.replace(day=1, hour=0, minute=0)):
                list.extend(RVCatalogue.listDataAfter(dir.getUrl(subdir), tm))
        files = dir.listFiles()
        for file in files:
            if RVCatalogue.getUTCTime(file) >= tm:
                # Add the complete URL so it can be retrieved
                list.append(dir.getUrl(file))
        return list