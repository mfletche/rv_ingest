from cassandra.cluster import Cluster

class CassSession:
    
    def __init__(self):
        cluster = Cluster()
        self.session = cluster.connect('bgp6')
        
        # Prepare statements to lower traffic and CPU utilization
        self.prepRibQuery = self.session.prepare('SELECT * FROM bgp6.importedrib WHERE file=\"?\"')
        self.prepSeqQuery = self.session.prepare('SELECT * from bgp6.bgpevents WHERE prefix=? AND ts=?')
        
    def hasBeenIngested(self, filename):
        """ Checks if an RIB file has already been ingested.
        """
        # TODO: Sanitize input.
        rows = self.session.execute(self.prepRibQuery, [filename])
        return rows
    
    def getNextSequenceNumber(self, prefix, ts):
        """ If multiple BGP events occur for a single prefix within one second
        the events will need to be ordered by their sequence numbers. The
        sequence numbers are not included in the MRT file so if there are
        already rows in the table with the same prefix and timestamp a unique,
        ascending sequence number must be added.
        """
        rows = self.session.execute('SELECT * from bgp6.bgpevents WHERE prefix=%s AND ')