from rv_catalogue import RVCatalogue
import arrow
import pycurl
from rib_file import BgpDump
from mrtparse import *
import mrt_file
import subprocess
from cassandra.cluster import Cluster
from cassandra import ReadTimeout

logoutput = sys.stdout

tmpname = 'tmp.csv'
r8_ip = '130.217.250.114'

# Schema for cassandra-loader
bgpevents_schema = '"bgp6.bgpevents(prefix,ts,sequence,peer,peerip,type,aspath)"'
rib_schema = '"bgp6.rib(prefix,peer,peerip,snapshot,ts,aspath)"'

# Copy statements for alternative method of bulk loading
events_copy = "COPY bgp6.bgpevents (prefix, ts, sequence, peer, peerip, type, aspath) FROM "
rib_copy = "COPY bgp6.rib (prefix, peer, peerip, snapshot, ts, aspath) FROM "
copy_options = " WITH DELIMITER = '|'"

# This class is just so I can construct an args object for Bgpdump manually
class Object(object):
    pass

# BGPDump arguments
bgpargs = Object()
bgpargs.verbose = False
bgpargs.ts_format = 'dump'
bgpargs.pkt_num = False

def fetch_file(url, tofile):
    with open(tofile, 'w') as local:
        c = pycurl.Curl()
        c.setopt(c.URL, url)
        c.setopt(c.WRITEDATA, local)
        c.perform()
        c.close()
        local.close()

def convert_mrt_to_csv(input, output, forceRIB=False):
    # Convert to CSV file
    d = Reader(input)
    bgpargs.output = open(tmpname, 'w')
    count = 0
    for m in d:
        m = m.mrt
        if m.err:
            continue
        b = BgpDump(bgpargs, forceRIB)
        if m.type == MRT_T['TABLE_DUMP']:
            b.td(m, count)
        elif m.type == MRT_T['TABLE_DUMP_V2']:
            b.td_v2(m)
        elif m.type == MRT_T['BGP4MP']:
            b.bgp4mp(m, count)
        count += 1

cluster = Cluster([r8_ip])
session = cluster.connect('bgp6')

for remotefile in RVCatalogue().listDataAfter(
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
        datafile = mrt_file.ProcessedRIBFile(localfile, tmpname, session)
        insert_q = rib_copy
    elif localfile.startswith('updates'):
        datafile = mrt_file.ProcessedUpdatesFile(localfile, tmpname, session)
        insert_q = events_copy
    else:
        sys.stderr.write('Cannot determine format: %s' % (localfile))
        exit()
    
    if datafile.is_file_inserted():
        logoutput.write('Already inserted file: %s\n' % (remotefile))
        continue
    
    logoutput.write('Fetching remote file: %s\n' % (remotefile))
    fetch_file(remotefile, localfile)
    logoutput.write('Fetched remote file: %s\n' % (remotefile))
    
    logoutput.write('Converting to CSV\n')
    if localfile.startswith('rib'):
        convert_mrt_to_csv(localfile, tmpname, forceRIB=True)
    else:
        convert_mrt_to_csv(localfile, tmpname)
    logoutput.write('Converted to CSV\n')
    
    logoutput.write('Beginning copy\n')
    cmd = 'cqlsh ' + r8_ip + ' -e "' + insert_q + ("'%s'" % tmpname) + copy_options + '"'
    print(cmd)
    r = subprocess.call(cmd, shell=True)
    logoutput.write('Copy finished\n')
    datafile.set_file_inserted(True)

    #loader_args = ['-f', '%s' % (tmpname), '-host', '130.217.250.114', '-schema', '%s' % (rib_schema)]
    
    # An alternative option to bulk load into Cassandra
    #r = subprocess.call(['/home/mfletche/opt/cassandra-loader', loader_args])
    #print('Return code: %s' % (r))