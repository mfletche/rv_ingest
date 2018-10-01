from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

class CassSession:
    
    def __init__(self):
        cluster = Cluster()
        self.session = cluster.connect('bgp6')
        
        # Prepare statements to lower traffic and CPU utilization
        insert = {}
        insert['bgpevents'] = 'INSERT INTO bgp6.bgpevents (prefix, ts, sequence, peer, peerip, type, aspath) VALUES (?, ?, ?, ?, ?, ?, ?)';
        insert['rib'] = 'INSERT INTO bgp6.rib (prefix, peer, peerip, snapshot, ts, aspath) VALUES (?, ?, ?, ?, ?, ?)';
        
    def ingest_rib_file():
        return
    
    def ingest_updates_file():
        return
    
    def is_file_already_ingested():
        return False