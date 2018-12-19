'''
Represents the state of a single peer in the network. A peer may advertise
routes to an AS and be part of an AS itself.

TODO: Should the peer have any notion of time? Should it know what the time
of its latest event was?

Created on 19/12/2018

@author: mfletche
'''

import pytricia

class Peer:
    def __init__(self, ip6_addresses):
        '''
        @param ip6_addresses: IP6 addresses of the interfaces of this peer. May
        be a single IP address or a list.  
        '''
        self.ip6_addresses = list(ip6_addresses)
        
        # A mapping of IPv6 address prefixes to the aspath announced by this
        # peer
        self.advertisements = pytricia.PyTricia(128)
        
        # The distance in hops from this peer to the border of an AS which it
        # has announced a route to. Negative numbers indicate that it is within
        # the AS itself.
        self.distance_to_as = {}
    
    def announce(self, prefix, path):
        self.advertisements[prefix] = path
    
    def has_path_to_prefix(self, prefix):
        return prefix in self.advertisements
    
    def hops_to_as(self, asn):
        return self.distance_to_as[asn]
    
    def lmp(self, prefix):
        ''' Finds the longest matching prefix advertised by this peer.
        Assuming that LMP is a common enough acronym that this will be easy
        to read.
        '''
        return self.advertisements.get_key(prefix)
    
    def path_to_prefix(self, prefix):
        ''' Returns the path to a prefix advertised by this peer.
        @raise KeyError: If the prefix is not advertised by this peer.
        '''
        return self.advertisements[prefix]
    
    def withdraw(self, prefix, path):
        del self.advertisements[prefix]