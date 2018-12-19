'''
Created on 17/12/2018

@author: mfletche
'''

import bgp6_db
from datetime import *
import pytricia
import re

IPV6_ADDR_LEN = 128

# Statements will only run once each per NetworkStateSnapshot created. If they
# must be run often, they should be PreparedStatements.

# These statements will run faster because they include the prefix
stmt_select_rib_prefix = 'SELECT * FROM rib WHERE prefix=%s AND year=%s AND snapshot=%s'
stmt_select_event_prefix = 'SELECT * FROM bgp_event WHERE prefix=%s AND year=%s AND month=%s AND time >= %s AND time <= %s ALLOW FILTERING'

# These statements can run fast if there is an index set up
stmt_select_rib = 'SELECT * FROM rib WHERE year=%s AND snapshot=%s ALLOW FILTERING'
stmt_select_event = 'SELECT * FROM bgp_event WHERE year=%s AND month=%s AND time >= %s AND time <= %s ALLOW FILTERING'

class NetworkStateSnapshot:
    
    def __init__(self, time, session, prefix):
        ''' Initializes a NetworkStateSnapshot object to the state at the given
        time.
        @param time: A date time object containing the time you would like the
        NetworkState object to represent.
        @param session: A Cassandra database session.  
        '''
        self.time = time
        self.prefix = prefix
        
        # Fast longest-matching prefix lookups
        self.prefixes = pytricia.PyTricia(IPV6_ADDR_LEN)
        
        # Will map prefixes to ASes
        self.advertised_ases = {}
        self.advertised_paths = {}
        
        # Contains peers with an active route
        self.active_peers = {}
        
        queries = NetworkStateSnapshot._get_bgp6_queries(time, prefix)
        
        # Get initial snapshot state from midnight RIB file
        for rib_row in session.execute(*queries[0]):
            rib_row = bgp6_db.RibRow(*rib_row)
            self.prefixes.insert(rib_row.prefix, rib_row.prefix)
            if rib_row.peer not in self.advertised_ases:
                self.advertised_ases[rib_row.peer] = {}
                
            self.advertised_paths[rib_row.peer] = rib_row.path
            ases = self._split_as_path(rib_row.path)
                    
            if isinstance(ases[-1], list):
                for asn in ases[-1]:
                    self.advertised_ases[rib_row.peer][asn] = 1
            else:
                self.advertised_ases[rib_row.peer][ases[-1]] = 1
        
        # Get any changes until the time we are interested in
        if len(queries) > 1:
            for event_row in session.execute(*queries[1]):
                event_row = bgp6_db.BgpEventRow(*event_row)
                if event_row.type == "W":
                    self.advertised_ases[event_row.peer] = {}
                    self.advertised_paths[event_row.peer] = None
                else:
                    if event_row.peer not in self.advertised_ases:
                        self.advertised_ases[event_row.peer] = {}
                    ases = self._split_as_path(event_row.path)
                    
                    if isinstance(ases[-1], list):
                        for asn in ases[-1]:
                            self.advertised_ases[event_row.peer][asn] = 1
                    else:
                        self.advertised_ases[event_row.peer][ases[-1]] = 1
                    self.advertised_ases[event_row.peer]
                    self.advertised_path[event_row.peer] = event_row.path
                
        for peer in self.advertised_ases.keys():
            self.active_peers[peer] = 1
    
    def _filter_updates(self, bgp_events):
        ''' Filters out bgp_events which are not relevant to the prefix of the
        NetworkStateSnapshot.
        '''
        updates = []
        
        for (time, peer, type, path) in bgp_events:
            
            # TODO: Clarify if this should be filtering out updates to a peer
            # when a path has been withdrawn. Shouldn't it update it if it is
            # announced again?
            if peer in self.advertised_paths and self.advertised_paths[peer] is not None:
                updates.append((time, peer, type, path,))
    
    @staticmethod
    def _get_bgp6_queries(time, prefix):
        ''' Sets up the queries for the bgp6 keyspace that will set up the
        NetworkState object. The returned list contains the query for the rib
        table in position 0 and the query for the bgp_event table in position
        1. The contents of the list can be executed directly by unpacking into
        the arguments of Session.execute() i.e. session.execute(*result[0]).
        
        If the time is the same as a RIB file (i.e. is at midnight UTC time),
        no bgp_event query will be generated
        @param time: The time that the snapshot will be set up for
        @param prefix: The prefix (may be None)
        '''
        assert(time is not None)
        
        rib_time = NetworkStateSnapshot._previous_utc_midnight(time)
        
        if prefix is not None:
            rib_query = stmt_select_rib_prefix
            rib_params = [prefix, rib_time.year, rib_time]
            if time != rib_time:
                event_query = stmt_select_event_prefix
                event_params = [prefix, rib_time.year, rib_time.month, rib_time, time]
        else:
            rib_query = stmt_select_rib
            rib_params = [rib_time.year, rib_time]
            if time != rib_time:
                event_query = stmt_select_event
                event_params = [rib_time.year, rib_time.month, rib_time, time]
            
        return [(rib_query, rib_params,),
                (event_query, event_params,)]
    
    
    @staticmethod
    def _previous_utc_midnight(time):
        ''' Get the midnight previous to the time provided. If the time is already
        midnight it will be unchanged.
        @param time: A timezone aware time object.
        '''
        assert(time.tzinfo is not None)
        
        utc_midnight = time.astimezone(timezone.utc)
        utc_midnight = utc_midnight.replace(hour=0, minute=0, second=0,
                                            microsecond=0)
        
        return utc_midnight
    
    @staticmethod
    def _split_as_path(aspath):
        ases = aspath.split()
        for i in range(len(ases)):
            # Convert to integer if possible
            try:
                ases[i] = int(ases[i])
            except ValueError:
                # Some will be in aggregate form "{x, y, z, ...}"
                pattern = '\d+'
                asns = re.findall(pattern, ases[i])
                
                # This should not fail because we've extracted only ints
                asns = [int(asn) for asn in asns]
                ases[i] = asns
        return ases
        
def main():
    db = bgp6_db.Bgp6Database()
    state = NetworkStateSnapshot(datetime(2018, 9, 25, 23, 45, tzinfo=timezone.utc),
                                db.session, prefix='2405:dc00:35e::/48')
    for prefix in state.prefixes:
        print(prefix)
        
    for peer in state.advertised_ases.keys():
        print(peer, state.advertised_ases[peer])
        
    for peer in state.active_peers.keys():
        print(peer)
    

if __name__ == '__main__':
    main()