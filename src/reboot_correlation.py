'''
Created on 11/12/2018

@author: mfletche
'''
import argparse
import datetime
import bz2
import gzip
import bgp6_db
import ipaddress
import pytricia
import re
import subprocess
import sys
from NetworkStateSnapshot import NetworkStateSnapshot

MIN_PREFIX_LEN = 16
MAX_PREFIX_LEN = 64

IPV6_LEN_BITS = 128

class Reboot:
    def __init__(self, reboot_start, reboot_end):
        if reboot_start > reboot_end:
            raise ValueError('Reboot start time must be before reboot end time')
        
        self.reboot_start = reboot_start
        self.reboot_end = reboot_end
        
    def _day_prior(self):
        return self.reboot_start - datetime.timedelta(days=1)
    
    def _day_after(self):
        return self.reboot_end + datetime.timedelta(days=1)
                
class Prefix:
    def __init__(self, prefix, distance=None):
        self.prefix = prefix
        self.distance = distance
        
class ProbesToRebootsCorrelator:
    def __init__(self, ip, reboots, ip_to_prefixes):
        self.ip = ip
        self.reboots = reboots
        self.ip_to_prefixes = ip_to_prefixes
        
    def _get_probes(self, session):
        stmt = 'SELECT probets, replyts, ipid FROM wart WHERE target=%s ' \
                'AND year=%s AND month=%s AND probets>=%s AND probets <= %s'
        
        warts_data = []
        
        for reboot in self.reboots:
            # Special case where two months have to be queried
            if reboot._day_prior().month != reboot._day_after().month:
                raise NotImplementedError
            else:
                warts_rows = session.execute(stmt, [self.ip, reboot.reboot_start.year,
                                       reboot.reboot_start.month,
                                       reboot._day_prior(),
                                       reboot._day_after()])
                for row in warts_rows:
                    warts_data.append((row[0], row[1], row[1],))
        return warts_data
    
    def _get_network_snapshot(self, reboot, session, prefix):
        snapshot = NetworkStateSnapshot(reboot._day_prior(), 
                                        session, prefix)
        return snapshot
        
    def _get_bgp_events_after_reboot(self, reboot, prefix, session):
        stmt = 'SELECT time, peer, type, path FROM bgp_event WHERE prefix=%s ' \
                'AND year=%s AND month=%s AND time >= %s AND time <= %s ALLOW FILTERING'
                
        events_rows = session.execute(stmt, [prefix, reboot._day_prior().year,
                               reboot._day_prior().month, reboot._day_prior(),
                               reboot._day_after()])

def get_value_from_csv_field(field):
    pattern = re.compile('^(.+?),')
    m = pattern.match(field)
    
    if not m: raise ValueError
    return m.group(1)

def is_comment(line):
    comment = re.compile('^#')
    return comment.match(line)
    
def prefix_length_in_range(prefix_len):
    ''' This module has a minimum and a maximum prefix length that it will
    store in the mapping structures. This function checks if the prefix length
    is in the allowed range.
    @param prefix_len: The length of an IPv6 prefix. Must be an int or
    castable to an int.
    @return: True if the prefix_len is within the range the module will store 
    '''
    num = int(prefix_len)
    
    if num >= MIN_PREFIX_LEN and num <= MAX_PREFIX_LEN:
        return True
    
    return False

class BgpPrefixMapping:
    
    def __init__(self):
        # Longest Matching Prefix (LMP)
        self.lmp_tree = pytricia.PyTricia(IPV6_LEN_BITS)
        self.prefix_asn_map = {}
        self.hops_from_border = {}
    
    class Line:
        def __init__(self, line):
            self.line = line
        
        def parse_line(self):
            (prefix, prefix_len, oases) = self.extract_fields()
            
            # Raises an error if prefix is not a valid IPV6 address
            ipaddress.IPv6Address(prefix)
            if not prefix_len_in_range(prefix_len): raise ValueError
            return (prefix, prefix_len, oases)
        
        def _extract_fields(self):
            pattern = re.compile('^(.+?)\s+(\d+)\s+([\d_]+$')
            m = pattern.match(self.line)
            
            if not m: raise ValueError
            
            netaddr = m.group(1)
            prefix_len = int(m.group(2))
            oases = m.group(3)
            
            return (netaddr, prefix_len, oases)
            
    def load_mapping(self, readable):
        for ln in readable:
            if is_comment(ln): continue
            
            line = PrefixToASMapping.Line(ln)
            (prefix, prefix_len, ases) = line.parse()
            prefix_str = "{}/{}".format(prefix, prefix_len)
            
            # TODO: When loading mapping from a RIB file, only the prefix-ASN
            # pair for the very last ASN in the list is set to one. Why is this
            # one different?
            for asn in ases.split('_'):
                self.prefix_asn_map[prefix_str][asn] = 1
                
        for prefix_str in self.prefix_asn_map.keys():
            self.lmp_tree.insert(prefix_str, prefix_str)

    def load_rib(self, bgp6_conn, rib_date):
        ''' Loads the contents of one RIB file from the Cassandra bgp6
        keyspace. The RIB file contains the currently advertised routes
        at midnight.
        
        Routers advertise prefixes and declare the path through ASes to
        reach that prefix. This function maps prefixes and the final AS
        in the advertised path. If the path to a prefix ends at an AS:
        
        BgpPrefixMapping.prefix_asn_map[prefix][as] will exist and will
        equal 1.
        '''
        results = bgp6_conn.session.execute(bgp6_conn.prep_stmt_select_snapshot,
                                  (rib_date,))
        
        # Results should be in (prefix, ases) tuples
        for ln in results:
            prefix_str = ln[0]
            (prefix, prefix_len) = prefix_str.split('/')
            if not prefix_length_in_range(prefix_len): continue
            ases = ln[1].split()
            if not prefix_str in self.prefix_asn_map:
                self.prefix_asn_map[prefix_str] = {}
                
            self.prefix_asn_map[prefix_str][ases[-1]] = 1
                
        for prefix_str in self.prefix_asn_map.keys():
            self.lmp_tree.insert(prefix_str, prefix_str)

def index_of_border_ip(traceroute_ips, dest_lmp, mapping):
    ''' Infers which interface is on the border of a network.
    @param traceroute_ips: A list of IPv6 addresses in the path to the
    destination network
    @param dest_lmp: The longest matching prefix of the destination network
    @param mapping: A BgpPrefixMapping object with the current state of the
    network fields
    '''
    for i in range(0, len(traceroute_ips)):
        try:
            ip = get_value_from_csv_field(traceroute_ips[i])
        except ValueError:
            continue
        
        try:
            ip_lmp = mapping.lmp_tree[ip]
        except KeyError:
            continue
        
        for asn in mapping.prefix_asn_map[ip_lmp]:
            if dest_lmp in mapping.prefix_asn_map and \
                        asn in mapping.prefix_asn_map[dest_lmp]:
                return i

def get_hops_from_border(ip_list, border_index, mapping, gaplimit, prefix):
    
    for i in range(0, len(ip_list)):
        ip = ip_list[i]
        
        try:
            ip = get_value_from_csv_field(ip_list[i])
        except ValueError:
            continue
        
        if not ip.startswith('2'): return
        
        if not border_index == None:
            diff = border_index - i - 1
            if not ip in mapping.hops_from_border:
                mapping.hops_from_border[ip] = {}
                
            if not prefix in mapping.hops_from_border[ip]:
                mapping.hops_from_border[ip][prefix] = {}
                
            if not 'diff' in mapping.hops_from_border[ip][prefix] or mapping.hops_from_border[ip][prefix]['diff'] > diff:
                mapping.hops_from_border[ip][prefix]['diff'] = diff
                
        elif gaplimit:
            diff = len(ip_list) - i
            
            if not ip in mapping.hops_from_border:
                mapping.hops_from_border[ip] = {}
                
            if not prefix in mapping.hops_from_border[ip]:
                mapping.hops_from_border[ip][prefix] = {}
            
            if not 'inpath' in mapping.hops_from_border[ip][prefix] or mapping.hops_from_border[ip][prefix]['inpath'] > diff:
                mapping.hops_from_border[ip][prefix]['inpath'] = diff
                
def parse_yyyymmdd_to_datetime(input):
    pattern = re.compile('^(\d{4})(\d{2})(\d{2})$')
    m = pattern.match(input)
    
    if not m: raise ValueError
    
    year, month, day = tuple(m.groups())
    result = datetime.datetime(int(year), int(month), int(day))
    return result

def process_trace_file(dump, mapping):
    lines = dump.splitlines()
    
    for ln in lines:
        ln = ln.decode('ascii')
        if is_comment(ln): continue
        
        fields = ln.split()
        if fields[0] != 'T' or fields[10] == 'L': continue
        
        gaplimit = (fields[10] == 'G')
        
        try:
            prefix = mapping.lmp_tree[fields[2]]
        except KeyError:
            continue
        
        border_index = index_of_border_ip(fields[13:], prefix, mapping)
        get_hops_from_border(fields[13:], border_index, mapping, gaplimit, prefix)

def do_sc_analysis_dump(filename):
    zcat = subprocess.Popen('zcat {}'.format(filename).split(), stdout=subprocess.PIPE)
    dump = subprocess.Popen('sc_analysis_dump', stdin=zcat.stdout, stdout=subprocess.PIPE)
    return dump.communicate()[0]

def main():
    parser = argparse.ArgumentParser(description='Map trace IPv6 addresses to prefix_asn_map.')
    parser.add_argument('--file', help='An IPtoASN file')
    parser.add_argument('--date', type=int, help='The date of a RIB file to import')
    
    # There must be at least one trace file, but also may be many.
    parser.add_argument('traces', nargs='+')
    
    prog_args = parser.parse_args()
    
    # The intention is that there should only be one of file or date, as they
    # provide the prefix-ASN mapping.
    if not (bool(prog_args.file) ^ bool(prog_args.date)):
        print('Either --file or --date is required, but not both.')
        parser.print_help()
        sys.exit()
    
    # Construct mapping
    mapping = BgpPrefixMapping()
    
    if prog_args.file:
        with bz2.open(prog_args.file, 'r') as f:
            mapping.load_mapping(f)
    else:
        # Date is in YYYYMMDD format
        db = bgp6_db.Bgp6Database()
        dt = parse_yyyymmdd_to_datetime(str(prog_args.date))
        mapping.load_rib(db, dt)
        
    # Process trace file
    for t in prog_args.traces:
        dump = do_sc_analysis_dump(t)
        process_trace_file(dump, mapping)
        
    list_ips = sorted(mapping.hops_from_border.keys())
    for ip in list_ips:
        ip_printed = False
        list_prefixes = sorted(mapping.hops_from_border[ip].keys())
        for prefix in list_prefixes:
            dist = None
            if 'diff' in mapping.hops_from_border[ip][prefix]:
                dist = mapping.hops_from_border[ip][prefix]['diff']
            elif 'inpath' in mapping.hops_from_border[ip][prefix]:
                dist = mapping.hops_from_border[ip][prefix]['inpath']
            
            if dist == None: continue
            # TODO: Implement max and min distances
            
            if not ip_printed:
                print(ip)
                ip_printed = True
            
            print('\t', prefix, dist)
            
if __name__ == '__main__':
    main()