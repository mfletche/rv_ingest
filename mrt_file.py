""" Represents each file type present on the routeview archive and contains
functions to INSERT them into a Cassandra db appropriately.

Modified version of mrt2bgpdump.py example from mrtparse
"""

from mrtparse import *
import time
import os
import copy

# This will be used as the value for the 'who' field
username = 'marianne'
peer = None

class SeqGenerator:
    """
    Class that will determine the sequence number of a line required for
    updates.
    """
    
    def __init__(self):
        self.dict = {}
    
    def get_seq(self, prefix, ts):
        """ Gets the sequence number for an update message. Calling this method
        will change its result on subsequent calls.
        """
        entry = self.dict.get(prefix)
        
        # If the prefix is new, or the timestamp has changed
        if not entry or entry[0] != ts:
            # Create entry with next seq number
            self.dict[prefix] = [ts, 1]
            return 0
        else:
            seq = entry[1]
            entry[1] += 1
            return seq

# These globals store information that must exist for longer than the parsing
# of a single record in the MRT file, but that also must be included in the
# output of the get_lines() function. They need to be reset at the beginning
# of each file read.
snapshot = None # Time of table dump
seq = SeqGenerator()

class MRTExtractor:
    """ The base class for specific types of MRT file. Extracts all data that
    might be required for subclasses.
    
    :param input: An object with a 'read' attribute or str containing the path
    to the MRT file.
    """
    def __init__(self, input):
        global seq, snapshot
        # Following is required by Reader class
        assert hasattr(input, 'read') or isinstance(input, str)
        self.reader = Reader(input)
        
        # Sequence and snapshot must be reset for each file
        seq = SeqGenerator()
        snapshot = None
        
    def lines(self, type):
        count = 0
        linecount = 0
        for m in self.reader:
            m = m.mrt
            if m.err:
                continue
            if type == 'RIB':
                p = RIBExtractor(m, count=count)
            elif type == 'Updates':
                p = UpdatesExtractor(m, count=count)
            else:
                sys.stderr.write('Error: Unsupported MRT line format.\n')
                return
            for line in p.lines():
                linecount += 1
                yield line

class MRTParser:
    """ A parser for MRT entries generated by MRTExtractor.mrt().
    """
    def __init__(self, mrt, count=None):
        global snapshot
        self.mrt = mrt
        self.count = count
        self.nlri = []
        self.withdrawn = []
        self.as4_path = []
        self.as_path = []
        
        # Load first timestamp in each file into global
        if not snapshot:
            snapshot = mrt.ts
        
    def bgp_attr(self, attr):
        if attr.type == BGP_ATTR_T['ORIGIN']:
            self.origin = ORIGIN_T[attr.origin]
        elif attr.type == BGP_ATTR_T['NEXT_HOP']:
            self.next_hop.append(attr.next_hop)
        elif attr.type == BGP_ATTR_T['AS_PATH']:
            self.as_path = []
            for seg in attr.as_path:
                if seg['type'] == AS_PATH_SEG_T['AS_SET']:
                    self.as_path.append('{%s}' % ','.join(seg['val']))
                elif seg['type'] == AS_PATH_SEG_T['AS_CONFED_SEQUENCE']:
                    self.as_path.append('(' + seg['val'][0])
                    self.as_path += seg['val'][1:-1]
                    self.as_path.append(seg['val'][-1] + ')')
                elif seg['type'] == AS_PATH_SEG_T['AS_CONFED_SET']:
                    self.as_path.append('[%s]' % ','.join(seg['val']))
                else:
                    self.as_path += seg['val']
        elif attr.type == BGP_ATTR_T['MULTI_EXIT_DISC']:
            self.med = attr.med
        elif attr.type == BGP_ATTR_T['LOCAL_PREF']:
            self.local_pref = attr.local_pref
        elif attr.type == BGP_ATTR_T['ATOMIC_AGGREGATE']:
            self.atomic_aggr = 'AG'
        elif attr.type == BGP_ATTR_T['AGGREGATOR']:
            self.aggr = '%s %s' % (attr.aggr['asn'], attr.aggr['id'])
        elif attr.type == BGP_ATTR_T['COMMUNITY']:
            self.comm = ' '.join(attr.comm)
        elif attr.type == BGP_ATTR_T['MP_REACH_NLRI']:
            self.next_hop = attr.mp_reach['next_hop']
            if self.type != 'BGP4MP':
                return
            for nlri in attr.mp_reach['nlri']:
                self.nlri.append('%s/%d' % (nlri.prefix, nlri.plen))
        elif attr.type == BGP_ATTR_T['MP_UNREACH_NLRI']:
            if self.type != 'BGP4MP':
                return
            for withdrawn in attr.mp_unreach['withdrawn']:
                self.withdrawn.append(
                    '%s/%d' % (withdrawn.prefix, withdrawn.plen))
        elif attr.type == BGP_ATTR_T['AS4_PATH']:
            self.as4_path = []
            for seg in attr.as4_path:
                if seg['type'] == AS_PATH_SEG_T['AS_SET']:
                    self.as4_path.append('{%s}' % ','.join(seg['val']))
                elif seg['type'] == AS_PATH_SEG_T['AS_CONFED_SEQUENCE']:
                    self.as4_path.append('(' + seg['val'][0])
                    self.as4_path += seg['val'][1:-1]
                    self.as4_path.append(seg['val'][-1] + ')')
                elif seg['type'] == AS_PATH_SEG_T['AS_CONFED_SET']:
                    self.as4_path.append('[%s]' % ','.join(seg['val']))
                else:
                    self.as4_path += seg['val']
        elif attr.type == BGP_ATTR_T['AS4_AGGREGATOR']:
            self.as4_aggr = '%s %s' % (attr.as4_aggr['asn'], attr.as4_aggr['id'])

    def parse_table_dump(self):
        self.type = 'TABLE_DUMP'
        self.flag = 'B'
        self.ts = m.ts
        self.num = count
        self.org_time = m.td.org_time
        self.peer_ip = m.td.peer_ip
        self.peer_as = m.td.peer_as
        self.nlri.append('%s/%d' % (m.td.prefix, m.td.plen))
        for attr in m.td.attr:
            self.bgp_attr(attr)
            
    def parse_table_dump_v2(self, m):
        global peer
        self.type = 'TABLE_DUMP2'
        self.flag = 'B'
        self.ts = m.ts
        if m.subtype == TD_V2_ST['PEER_INDEX_TABLE']:
            peer = copy.copy(m.peer.entry)
        elif (m.subtype == TD_V2_ST['RIB_IPV4_UNICAST']
            or m.subtype == TD_V2_ST['RIB_IPV4_MULTICAST']
            or m.subtype == TD_V2_ST['RIB_IPV6_UNICAST']
            or m.subtype == TD_V2_ST['RIB_IPV6_MULTICAST']):
            self.num = m.rib.seq
            self.nlri.append('%s/%d' % (m.rib.prefix, m.rib.plen))
            for entry in m.rib.entry:
                self.org_time = entry.org_time
                self.peer_ip = peer[entry.peer_index].ip
                self.peer_as = peer[entry.peer_index].asn
                self.as_path = []
                self.origin = ''
                self.next_hop = []
                self.local_pref = 0
                self.med = 0
                self.comm = ''
                self.atomic_aggr = 'NAG'
                self.aggr = ''
                self.as4_path = []
                self.as4_aggr = ''
                for attr in entry.attr:
                    self.bgp_attr(attr)
                yield True
                    
    def parse_bgp4mp(self, m, count):
        self.type = 'BGP4MP'
        self.ts = m.ts
        self.num = count
        self.org_time = m.ts
        self.peer_ip = m.bgp.peer_ip
        self.peer_as = m.bgp.peer_as
        if (m.subtype == BGP4MP_ST['BGP4MP_STATE_CHANGE']
            or m.subtype == BGP4MP_ST['BGP4MP_STATE_CHANGE_AS4']):
            self.flag = 'STATE'
            self.old_state = m.bgp.old_state
            self.new_state = m.bgp.new_state
            self.print_line([], '')
        elif (m.subtype == BGP4MP_ST['BGP4MP_MESSAGE']
            or m.subtype == BGP4MP_ST['BGP4MP_MESSAGE_AS4']
            or m.subtype == BGP4MP_ST['BGP4MP_MESSAGE_LOCAL']
            or m.subtype == BGP4MP_ST['BGP4MP_MESSAGE_AS4_LOCAL']):
            if m.bgp.msg.type != BGP_MSG_T['UPDATE']:
                return
            for attr in m.bgp.msg.attr:
                self.bgp_attr(attr)
            for withdrawn in m.bgp.msg.withdrawn:
                self.withdrawn.append(
                    '%s/%d' % (withdrawn.prefix, withdrawn.plen))
            for nlri in m.bgp.msg.nlri:
                self.nlri.append('%s/%d' % (nlri.prefix, nlri.plen))
                
    def lines(self):
        """ Generates data that would appear in each line of BGPdump ouMRTExtractortput.
        """

        if self.mrt.type == MRT_T['TABLE_DUMP']:
            self.parse_table_dump(self.mrt, self.count)
            for route in self.print_routes():
                yield route
        elif self.mrt.type == MRT_T['TABLE_DUMP_V2']:
            for state in self.parse_table_dump_v2(self.mrt):
                for route in self.print_routes():
                    yield route
        elif self.mrt.type == MRT_T['BGP4MP']:
            self.parse_bgp4mp(self.mrt, self.count)
            for route in print_routes():
                yield route

    def print_routes(self):
        # The subclasses must have a 'get_line' attribute defined.
        assert hasattr(self, 'get_line')
        for withdrawn in self.withdrawn:
            if self.type == 'BGP4MP':
                self.flag = 'W'
            yield self.get_line(withdrawn, '')
        for nlri in self.nlri:
            if self.type == 'BGP4MP':
                self.flag = 'A'
            for next_hop in self.next_hop:
                yield self.get_line(nlri, next_hop)
                
    def merge_as_path(self):
        if len(self.as4_path):
            n = len(self.as_path) - len(self.as4_path)
            return ' '.join(self.as_path[:n] + self.as4_path)
        else:
            return ' '.join(self.as_path)

# These subclasses will extract specific data fields from the MRTExtractor and
# return them in a tuple.

class RIBExtractor(MRTParser):
    """ Represents a RIB file.
    """    
    def __init__(self, mrt, count=None):
        MRTParser.__init__(self, mrt, count)
        
    def get_line(self, prefix, next_hop):
        global snapshot
        """ Get a line of data for the RIB table.
        """
        return (prefix, int(self.peer_as), self.peer_ip, int(snapshot) * 1000,
                int(self.ts) * 1000, self.merge_as_path())

class UpdatesExtractor(MRTParser):
    """ Represents an Updates file.
    """
    
    def __init__(self, mrt, count=None):
        MRTParser.__init__(self, mrt, count)
        
    def get_line(self, prefix, next_hop):
        global seq
        return (prefix, int(self.ts) * 1000, seq.get_seq(prefix, self.ts), int(self.peer_as), self.peer_ip, self.flag, self.merge_as_path())

def main():
    if not len(sys.argv) == 2:
        sys.stderr.write('Not enough arguments.\n')
        return
    ext = MRTExtractor(sys.argv[1])
    if (sys.argv[1].startswith('rib')):
        type = 'RIB'
    elif (sys.argv[1].startswith('updates')):
        type = 'Updates'
    else:
        sys.stderr.write('Unrecognized file type.\n')
        return
    
    for line in ext.lines(type):
        print(line)
        
if __name__ == '__main__':
    main()
    