from rv_catalogue import RVCatalogue
import bgp6_db
import mrt_file
import os
import sys
import arrow
import pycurl
import time_uuid
import cassandra.concurrent
import io
from tqdm import tqdm
from mrtparse import *
import datetime

db = bgp6_db.Bgp6Database()

#try:
    # Where logging messages will be written
#    logoutput = open('tmp.txt', 'a+')
#except IOError as e:
#    print("I/O error ({0}): {1}".format(e.errno, e.strerror))
logoutput = sys.stdout
#except:
    # Unexpected error when opening file.
#    print("Unexpected error:", sys.exc_info()[0])
#    logoutput = sys.stdout

def update_to_bgp_event_row(update):
    event_timestamp = update.time
    event_datetime = datetime.datetime.fromtimestamp(event_timestamp)
    
    row = bgp6_db.BgpEventRow(
        prefix=update.prefix,
        time=time_uuid.TimeUUID.with_timestamp(event_timestamp),
        year=event_datetime.year,
        month=event_datetime.month,
        peer=update.peer_ip,
        asn=update.peer_as,
        path=update.as_path,
        type=update.type,
        seq=None
        )
    
    return row

def update_to_rib_row(update, snapshot_datetime):
    row = bgp6_db.RibRow(
        prefix=update.prefix,
        year=snapshot_datetime.year,
        snapshot=snapshot_datetime,
        peer=update.peer_ip,
        asn=update.peer_as,
        path=update.as_path,
        ts=update.time)
    
    return row

def fetch_file(url, tofile):
    """ Fetches a remote file and stores it as a local file.
    
    :param url: The url to fetch the file from.
    :param tofile: The path to write the file to.
    :return: 
    """
    with open(tofile, 'wb') as local:
        
        c = pycurl.Curl()
        c.setopt(c.URL, url)
        c.setopt(c.WRITEFUNCTION, local.write)
        c.perform()
        
        # Check response
        response = c.getinfo(c.RESPONSE_CODE)
                    
        c.close()
        return response
        

for remotefile in tqdm(RVCatalogue.listDataAfter(
    'http://archive.routeviews.org/route-views6/bgpdata/',
    arrow.get(2018, 9, 25, 0, 0))):
    
    # Work out filename
    localfile = remotefile.rsplit('/', 1)[-1]
    #localfile = localfile.encode('utf-8')   # localfile was a Unicode string
    
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
    
    if not db.is_file_ingested(localfile, bgp6_db.IMPORTED_RIB_TABLE_NAME if type == 'RIB'
                               else bgp6_db.IMPORTED_UPDATES_TABLE_NAME):
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
        
        # Queue for big concurrent insertion operation
        rib_insert_queue = []
        updates_insert_queue = []
        
        d = Reader(localfile)
        count = 0
        for mrt in d:
            lines = []
            mrt = mrt.mrt
            if mrt.err:
                continue
            bgpdump = mrt_file.BgpDump()
            if mrt.type == MRT_T['TABLE_DUMP']:
                lines += bgpdump.td(mrt, count)
            elif mrt.type == MRT_T['TABLE_DUMP_V2']:
                lines += bgpdump.td_v2(mrt)
            elif mrt.type == MRT_T['BGP4MP']:
                lines += bgpdump.bgp4mp(mrt, count)
            count += 1
            
            for ln in lines:
                if type == 'RIB':
                    rib_insert_queue.append(update_to_rib_row(ln, tm.datetime))
                else:
                    updates_insert_queue.append(update_to_bgp_event_row(ln))

        try:
            rib_len = len(rib_insert_queue)
            if rib_len:
                print('Inserting %d RIB entries...' % (rib_len))
            cassandra.concurrent.execute_concurrent_with_args(db.session, db.prep_stmt_insert_rib, rib_insert_queue)
            
            event_len = len(updates_insert_queue)
            if event_len:
                print('Inserting %d UPDATES entries...' % (event_len))
            cassandra.concurrent.execute_concurrent_with_args(db.session, db.prep_stmt_insert_bgpevents, updates_insert_queue)

            # Write final value
            logoutput.write('\rEntries: %s\n' % count)
    
            logoutput.write('Completed ingesting file: %s\n' % localfile)
            db.set_file_ingested(localfile, True, bgp6_db.IMPORTED_RIB_TABLE_NAME if type == 'RIB'
                                 else bgp6_db.IMPORTED_UPDATES_TABLE_NAME)
        except Exception as exc:
            exc.print_stack_trace()
        
        os.remove(localfile)    # Clean up
        
if not logoutput == sys.stdout:
    logoutput.close()