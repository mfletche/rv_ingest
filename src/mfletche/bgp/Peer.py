'''
Represents the state of a single peer in the network. A peer may advertise
routes to an AS and be part of an AS itself.

Created on 19/12/2018

@author: mfletche
'''

import pytricia

class Peer:
    def __init__(self, aliases):
        '''
        @param aliases: A list of IPv6 addresses that correspond to interfaces
        on the same physical peer. 
        '''
        self.aliases = aliases
        
        # A mapping of IPv6 address prefixes to the aspath announced by this
        # peer
        self.advertisements = pytricia.PyTricia()
        
        # The distance in hops from this peer to the border of an AS which it
        # has announced a route to. Negative numbers indicate that it is within
        # the AS itself.
        self.hops_to_border = {}