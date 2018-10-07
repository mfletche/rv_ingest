from rv_catalogue import RVCatalogue
from cass_interface import CassInterface
import mrt_file
import os
import sys
import arrow
import pycurl

RIB_META_NAME = 'importedrib'
UPDATES_META_NAME = 'imported'

db = CassInterface()

try:
    # Where logging messages will be written
    logoutput = open('tmp.txt', 'a+')
except IOError as e:
    print "I/O error ({0}): {1}".format(e.errno, e.strerror)
    logoutput = sys.stdout
except:
    # Unexpected error when opening file.
    print "Unexpected error:", sys.exc_info()[0]
    logoutput = sys.stdout

def fetch_file(url, tofile):
    """ Fetches a remote file and stores it as a local file.
    
    :param url: The url to fetch the file from.
    :param tofile: The path to write the file to.
    :return: 
    """
    with open(tofile, 'w') as local:
        c = pycurl.Curl()
        c.setopt(c.URL, url)
        c.setopt(c.WRITEDATA, local)
        try:
            c.perform()
            
            # Check response
            response = c.getinfo(c.RESPONSE_CODE)
            c.close()
            return response
        
        except pycurl.error as e:
            # https://curl.haxx.se/libcurl/c/libcurl-errors.html
            # There doesn't seem to be much that can be done if any of these
            # errors occur. If an error occurs with curl itself, None will be
            # returned from this function rather than the HTTP response code.
            errno, errstr = error
            logoutput.write(errstr)

for remotefile in RVCatalogue.listDataAfter(
    'http://archive.routeviews.org/route-views6/bgpdata/',
    arrow.get(2018, 9, 25, 0, 0)):
    
    # Work out filename
    localfile = remotefile.rsplit('/', 1)[-1]
    localfile = localfile.encode('utf-8')   # localfile was a Unicode string
    
    if localfile.startswith('rib'):
        # Only fetch RIB files which have a midnight timestamp
        tm = RVCatalogue().getUTCTime(localfile)
        if not (tm.hour == 0 and tm.minute == 0):
            continue 
        
    if localfile.startswith('rib'):
        type = 'RIB'
    elif localfile.startswith('updates'):
        type = 'Updates'
    else:
        logoutput.write('Cannot determine format: %s' % (localfile))
        continue
    
    if not db.is_file_ingested(localfile, RIB_META_NAME if type == 'RIB' else UPDATES_META_NAME):
        # File may already be here
        if not os.path.isfile(localfile):
            # Do the actual fetching of the file
            logoutput.write('Fetching remote file: %s\n' % (remotefile))
            response = fetch_file(remotefile, localfile)
            if response == None:
                # Server could not be reached or serious problem with Curl
                # If there are connectivity problems give up and try later.
                logoutput.write('Could not retrieve file: %s\nEXITING\n')
                exit()
            elif not response == 200:
                logoutput.write('ERROR: Could not fetch file: %s\nRESPONSE CODE: %d' % (localfile, response))
                continue
            
            logoutput.write('Fetched remote file: %s\n' % (remotefile))
        
        logoutput.write('Ingesting file: %s\n' % (localfile))
        
        # Parse into lines and insert them into db
        mrtfile = mrt_file.MRTExtractor(localfile)
        count = 0
        for line in mrtfile.lines(type):
            count += 1
            if type == 'RIB':
                db.insert_rib(line)
            else:
                db.insert_updates(line)
            if count % 1000 == 0:
                logoutput.write('\rEntries: %s' % count)

        # Write final value
        logoutput.write('\rEntries: %s\n' % count)

        logoutput.write('Completed ingesting file: %s\n' % localfile)
        db.set_file_ingested(localfile, True, RIB_META_NAME if type == 'RIB' else UPDATES_META_NAME)
        os.remove(localfile)    # Clean up
        
if not logoutput == sys.stdout:
    logoutput.close()