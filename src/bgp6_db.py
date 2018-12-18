from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT
import cassandra
from cassandra.cluster import ExecutionProfile
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
import time
from recordclass import recordclass

DEFAULT_NODE_IP = '130.217.250.114'
DEFAULT_KEYSPACE = 'bgp6'
DEFAULT_WHO = 'marianne'

# Both meta tables (tables which store information about files imported) have
# the same schema.
# The names of these columns can be changed without modifying code elsewhere,
# but not the order.
COLUMNS_META = ['time', 'who', 'file']

ImportRow = recordclass('ImportRow', ['ts', 'who', 'file'])

BGP6_KEYSPACE_CREATE = ('CREATE KEYSPACE IF NOT EXISTS {} '
                        'WITH REPLICATION = {{'
                        "'class': 'SimpleStrategy', "
                        "'replication_factor': '2'}} "
                        'AND durable_writes = true;')

''' The RIB table buckets by year so that partitions will not become too large.
Only one RIB file is intended to be imported per day so data arrives relatively
slowly. However, if there was no bucketing by time the partition size would
grow unbounded.

The most recent entries (for each prefix) will be stored first.
'''
RIB_TABLE_NAME = 'rib'
class RibEntry(Model):
    __table_name__ = RIB_TABLE_NAME
    __keyspace__ = DEFAULT_KEYSPACE
    
    prefix = columns.Text(primary_key=True)
    year = columns.SmallInt(primary_key=True)
    snapshot = columns.DateTime(primary_key=True, clustering_order="DESC")
    peer = columns.Inet(primary_key=True, clustering_order="ASC")
    asn = columns.Integer()
    path = columns.Text(primary_key=True)
    ts = columns.DateTime()

RIB_TABLE_CREATE = ('CREATE TABLE IF NOT EXISTS {} ('
                    'prefix text, '
                    'year smallint, '
                    'snapshot timestamp, '
                    'peer inet, '
                    'asn int, '
                    'path text, '
                    'ts timestamp, '
                    'PRIMARY KEY ((prefix, year), snapshot, peer, path)'
                    ') WITH CLUSTERING ORDER BY (snapshot DESC, peer ASC);')
COLUMNS_RIB = ['prefix', 'year', 'snapshot', 'peer', 'path', 'asn', 'ts']
RibRow = recordclass('RibRow', COLUMNS_RIB)

RIB_INSERT = ('INSERT INTO %s (%s) '
            'VALUES (%s)' % (RIB_TABLE_NAME, ', '.join(COLUMNS_RIB),
                             ', '.join(list('?'*len(COLUMNS_RIB)))))

''' Using a SASIIndex allows selecting snapshots using < or > comparison
operators.
'''
RIB_SNAPSHOT_INDEX = "CREATE CUSTOM INDEX rib_snapshot ON {} (snapshot) USING " \
    "'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {{ " \
    "'mode': 'SPARSE'}};".format(RIB_TABLE_NAME)
RIB_SNAPSHOT_RANGE_SELECT = "SELECT prefix, path FROM {} WHERE snapshot >= ? " \
    "AND snapshot <= ? ALLOW FILTERING;".format(RIB_TABLE_NAME)
RIB_SNAPSHOT_SELECT = "SELECT prefix, path FROM {} WHERE " \
    "snapshot=toTimestamp((date)?) ALLOW FILTERING".format(RIB_TABLE_NAME)

''' The BGPEvent table buckets by month as data arrives at a fast enough rate
that some partitions will become larger than 100 Mb within that time.

The time column is a timeuuid because multiple events may occur for a single
prefix during the same timestamp (one millisecond). This ensures uniqueness and
ordering.

The most recent entries (for each prefix) will be stored first.
'''
BGPEVENT_TABLE_NAME = 'bgp_event'
class BgpEvent(Model):
    __table_name__ = BGPEVENT_TABLE_NAME
    __keyspace__= DEFAULT_KEYSPACE
    
    prefix = columns.Text(primary_key=True)
    year = columns.SmallInt(primary_key=True)
    month = columns.TinyInt(primary_key=True)
    time = columns.DateTime(primary_key=True, clustering_order="DESC")
    seq = columns.Integer(primary_key=True)
    peer = columns.Inet()
    asn = columns.Integer()
    path = columns.Text()
    type = columns.Text()

BGPEVENT_TABLE_CREATE = ('CREATE TABLE IF NOT EXISTS {} ('
                      'prefix text, '
                      'year smallint, '
                      'month tinyint, '
                      'time timestamp, '
                      'seq int, '
                      'peer inet, '
                      'asn int, '
                      'path text, '
                      'type text, '
                      'PRIMARY KEY((prefix, year, month), time, seq)'
                      ') WITH CLUSTERING ORDER BY (time DESC);')
COLUMNS_BGPEVENTS = ['prefix', 'year', 'month', 'time', 'seq', 'asn', 'path', 'peer', 'type']
BgpEventRow = recordclass('BgpEventRow', COLUMNS_BGPEVENTS)

BGPEVENT_INSERT = ('INSERT INTO %s (%s) '
                   'VALUES (%s)' % (BGPEVENT_TABLE_NAME, ", ".join(COLUMNS_BGPEVENTS),
                   ", ".join(list('?'*len(COLUMNS_BGPEVENTS)))))

IMPORTED_RIB_TABLE_NAME = 'imported_rib'
class ImportedRibFile(Model):
    __table_name__ = IMPORTED_RIB_TABLE_NAME
    __keyspace__ = DEFAULT_KEYSPACE
    
    file = columns.Text(primary_key=True)
    time = columns.DateTime()
    who = columns.Text()
    
IMPORTED_RIB_TABLE_CREATE = ('CREATE TABLE IF NOT EXISTS {} ('
                          'file text PRIMARY KEY, '
                          'time timestamp, '
                          'who text);')
COLUMNS_IMPORTED_RIB = ['file', 'time', 'who']
ImportedRibRow = recordclass('ImportedRibRow', COLUMNS_IMPORTED_RIB)

IMPORTED_UPDATES_TABLE_NAME = 'imported_updates'
class ImportedUpdatesFile(Model):
    __table_name__ = IMPORTED_UPDATES_TABLE_NAME
    __keyspace__ = DEFAULT_KEYSPACE
    
    file = columns.Text(primary_key=True)
    time = columns.DateTime()
    who = columns.Text()

IMPORTED_UPDATES_TABLE_CREATE = ('CREATE TABLE IF NOT EXISTS {} ('
                              'file text PRIMARY KEY, '
                              'time timestamp, '
                              'who text);')
COLUMNS_IMPORTED_UPDATES = COLUMNS_IMPORTED_RIB.copy()
ImportedUpdatesRow = recordclass('ImportedUpdatesRow', COLUMNS_IMPORTED_UPDATES)

MAX_ASYNC_REQUESTS = 8

class Bgp6Database:
    """ Acts as an interface to the bgp6 keyspace in the Cassandra database.
    This file must be changed if any of the schemas change.
    """
    def __init__(self, ip=DEFAULT_NODE_IP, keyspace=DEFAULT_KEYSPACE,
                 who=DEFAULT_WHO):
        try:
            cluster = Cluster([ip])
            self.session = cluster.connect(keyspace)
        except cassandra.cluster.NoHostAvailable:
            # May not be set up
            print('Performing first time setup...')
            self.first_time_setup(ip, keyspace)
        
        self.who = who
        
        # Prepared statements for very common queries
        self.prep_stmt_insert_rib = self.session.prepare(RIB_INSERT)
        self.prep_stmt_insert_bgpevents = self.session.prepare(BGPEVENT_INSERT)
        self.prep_stmt_select_snapshot = self.session.prepare(RIB_SNAPSHOT_SELECT)
        
        # A list of all ResponseFuture objects which have not been checked yet.
        self.futures = []
    
    def create_keyspace(self, keyspace):
        ''' Create a keyspace for the BGP tables.
        '''
        
        stmt = BGP6_KEYSPACE_CREATE.format(keyspace)
        
        # Connect without keyspace
        self.session.execute(stmt)
        
    def first_time_setup(self, node_ip=DEFAULT_NODE_IP, keyspace=DEFAULT_KEYSPACE):
        # Keyspace may not exist
        cluster = Cluster([DEFAULT_NODE_IP])
        self.session = cluster.connect()
        self.create_keyspace(keyspace)
        self.session.set_keyspace(keyspace)
        
        self.session.execute(RIB_TABLE_CREATE.format(RIB_TABLE_NAME))
        self.session.execute(BGPEVENT_TABLE_CREATE.format(BGPEVENT_TABLE_NAME))
        self.session.execute(IMPORTED_RIB_TABLE_CREATE.format(IMPORTED_RIB_TABLE_NAME))
        self.session.execute(IMPORTED_UPDATES_TABLE_CREATE.format(IMPORTED_UPDATES_TABLE_NAME))
        
    def insert_rib(self, values):
        """ Insert a line of RIB data into the database.
        :param values: A list containing the values to be inserted.
        """
        assert len(values) == len(COLUMNS_RIB)
        bound = self.prep_stmt_insert_rib.bind(values)
        self.futures.append(self.session.execute_async(bound))
        if (len(self.futures) > MAX_ASYNC_REQUESTS):
            self.check_deferred_responses()
    
    def insert_updates(self, values):
        """ Insert a line of Updates data into the database.
        :param values: A list containing the values to be inserted.
        """
        assert len(values) == len(COLUMNS_BGPEVENTS)
        bound = self.prep_stmt_insert_bgpevents.bind(values)
        self.futures.append(self.session.execute_async(bound))
        if (len(self.futures) > MAX_ASYNC_REQUESTS):
            self.check_deferred_responses()
    
    def set_file_ingested(self, original_name, ingested, tablename):
        """ Insert or delete a row in one of the 'meta' data tables which
        indicates that a file has been ingested.
        
        :param original_name: A string, the name of the file that the processed
        data came from.
        :param ingested: A boolean, whether the row should exist in the table.
        :param tablename: A string, the name of the table to insert the row
        into -- since both of the tables have the same schema.
        """
        if ingested:
            prep_stmt = self.session.prepare(
                'INSERT INTO {0} ({1}) VALUES (?, ?, ?)'.format(
                    tablename, ",".join(COLUMNS_META)
                ))
            bound = prep_stmt.bind([int(time.time()) * 1000, self.who, original_name])
        else:
            prep_stmt = self.session.prepare(
                'DELETE FROM {0} WHERE {1}=?'.format(tablename, COLUMNS_META[2])
                )
            bound = prep_stmt.bind([original_name])
        # This is not asynchronous since this will be sent once per large file.
        self.session.execute(bound)
    
    def is_file_ingested(self, original_name, tablename):
        """ Query the table to determine if a file with the given name has
        already been ingested.
        
        :param original_name: A string, the name of the file that the processed
        data came from.
        :param tablename: A string, the name of the table to query.
        """
        prep_stmt = self.session.prepare(
            'SELECT * FROM {0} WHERE {1}=?'.format(tablename, COLUMNS_META[2])
        )
        bound = prep_stmt.bind([original_name])
        results = self.session.execute(bound)
        return True if len(results.current_rows) > 0 else False
    
    def check_deferred_responses(self):
        """ Checks ResponseFuture objects stored in self.futures. Exceptions
        that occurred during async query executions will occur here.
        """
        for future in self.futures:
            results = future.result()
            
        self.futures = []

def main():
    '''
    '''
        
if __name__ == '__main__':
    main()