from rv_catalogue import RVCatalogue
import arrow
import pycurl
from rib_file import BgpDump
from mrtparse import *
import subprocess
from cassandra.cluster import Cluster

tmpname = 'tmp.csv'
bgpevents_schema = '"bgp6.bgpevents(prefix,ts,sequence,peer,peerip,type,aspath)"'
rib_schema = '"bgp6.rib(prefix,peer,peerip,snapshot,ts,aspath)"'

cluster = Cluster()
session = cluster.connect('bgp6')

events_insert = session.prepare('INSERT INTO bgp6.bgpevents ' \
    + '(prefix, ts, sequence, peer, peerip, type, aspath) VALUES ' \
    + '(\"?\", ?, ?, ?, \"?\", \"?\", \"?\")')
rib_insert = session.prepare('INSERT INTO bgp6.rib ' \
    + '(prefix, peer, peerip, snapshot, ts, aspath) VALUES ' \
    + '(\"?\", ?, \"?\", ?, ?, \"?\")')
imported_insert = session.prepare('INSERT INTO bgp6.imported ' \
    + '(ts, who, file) VALUES (?, \"?\", \"?\")')
importedrib_insert = session.prepare('INSERT INTO bgp6.importedrib ' \
    + '(ts, who, filename) VALUES (?, \"?\", \"?\")')

# This class is just so I can construct an args object manually
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
    
def convert_mrt_to_csv(input, output):
    # Convert to CSV file
    print(type(input))
    print(input)
    d = Reader(input)
    bgpargs.output = open(tmpname, 'wb')
    count = 0
    for m in d:
        m = m.mrt
        if m.err:
            continue
        b = BgpDump(bgpargs)
        if m.type == MRT_T['TABLE_DUMP']:
            b.td(m, count)
        elif m.type == MRT_T['TABLE_DUMP_V2']:
            b.td_v2(m)
        elif m.type == MRT_T['BGP4MP']:
            b.bgp4mp(m, count)
        count += 1

#for remotefile in RVCatalogue().listDataAfter(
#    'http://archive.routeviews.org/route-views6/bgpdata/',
#    arrow.get(2018, 9, 25, 0, 0)):
    
    # Work out filename
    #localfile = remotefile.rsplit('/', 1)[-1]
    #localfile = localfile.encode('utf-8')   # localfile was a Unicode string
    
#localfile = 'rib.20180925.0000.bz2'
#if localfile.startswith('rib'):
    # Only fetch RIB files which have a midnight timestamp
#    tm = RVCatalogue().getUTCTime(localfile)
#    if not (tm.hour == 0 and tm.minute == 0):
#        exit() 
    
    #fetch_file(remotefile, localfile)

#convert_mrt_to_csv(localfile, tmpname)

# Insert items from the CSV file into the Cassandra database. There is a faster
# way to do this (bulk loading) but that will take a bit of extra work

if localfile.startswith('rib'):
    insert_q = rib_insert
elif localfile.startswith('updates'):
    insert_q = events_insert
else:
    sys.stderr.write('Cannot determine format: %s' % (localfile))
    exit()

with open(tmpname) as fp:
    line = fp.readline()
    while line:
        future = session.execute_async(insert_q, line.split(','))

#loader_args = '-fake -f %s -host 130.217.250.114 -schema %s' % (tmpname, rib_schema)

# Bulk load into Cassandra
#r = subprocess.call(['/home/mfletche/opt/cassandra-loader', loader_args])
#print('Return code: %s' % (r))