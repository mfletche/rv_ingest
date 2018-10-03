from rv_catalogue import RVCatalogue
from cass_interface import CassInterface
import mrt_file
import os
import sys
import arrow
import pycurl

RIB_META_NAME = 'importedrib'
UPDATES_META_NAME = 'imported'

def fetch_file(url, tofile):
    with open(tofile, 'w') as local:
        c = pycurl.Curl()
        c.setopt(c.URL, url)
        c.setopt(c.WRITEDATA, local)
        c.perform()
        c.close()
        local.close()

db = CassInterface()
logoutput = sys.stdout

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
        sys.stderr.write('Cannot determine format: %s' % (localfile))
        continue
    
    if not db.is_file_ingested(localfile, RIB_META_NAME if type == 'RIB' else UPDATES_META_NAME):
        # File may already be here
        if not os.path.isfile(localfile):
            # Do the actual fetching of the file
            logoutput.write('Fetching remote file: %s\n' % (remotefile))
            fetch_file(remotefile, localfile)
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
            if count % 100000:
                logoutput.write('\rEntries: %s' % count)

        # Write final value
        logoutput.write('\rEntries: %s\n' % count)

        logoutput.write('Completed ingesting file: %s\n' % localfile)
        db.set_file_ingested(localfile, True, RIB_META_NAME if type == 'RIB' else UPDATES_META_NAME)
        os.remove(localfile)    # Clean up