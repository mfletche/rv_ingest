from cassandra.cluster import Cluster

DEFAULT_NODE_IP = '130.217.250.114'
DEFAULT_KEYSPACE = 'bgp6'
DEFAULT_WHO = 'marianne'

# Column names for tables
NAME_RIB = 'rib'
COLUMNS_RIB = ['prefix', 'peer', 'peerip', 'snapshot', 'ts', 'aspath']
NAME_BGPEVENTS = 'bgpevents'
COLUMNS_BGPEVENTS = ['prefix', 'ts', 'sequence', 'peer', 'peerip', 'type', 'aspath']

# Both meta tables (tables which store information about files imported) have
# the same schema.
# The names of these columns can be changed without modifying code elsewhere,
# but not the order.
COLUMNS_META = ['ts', 'who', 'file']

class CassInterface:
    """ Acts as an interface to the bgp6 keyspace in the Cassandra database.
    This file must be changed if any of the schemas change.
    """
    def __init__(self, ip=DEFAULT_NODE_IP, keyspace=DEFAULT_KEYSPACE,
                 who=DEFAULT_WHO):
        cluster = Cluster()
        if keyspace:
            self.session = cluster.connect(keyspace)
        else:
            self.session = cluster.connect()
        self.who = who
        
        # Prepared statements for very common queries
        self.prep_stmt_insert_rib = session.prepare(
            'INSERT INTO %s (%s) '
            'VALUES (%s)' % (NAME_RIB, ', '.join(COLUMNS_RIB),
                             ', '.join(list('?'*len(COLUMNS_RIB))))
            )
        self.prep_stmt_insert_bgpevents = session.prepare(
            'INSERT INTO %s (%s) '
            'VALUES (%s)' % (NAME_BGPEVENTS, ", ".join(COLUMNS_BGPEVENTS),
                             ", ".join(list('?'*len(COLUMNS_BGPEVENTS))))
            )
    
    def insert_rib(self, values):
        """ Insert a line of RIB data into the database.
        :param values: A list containing the values to be inserted.
        """
        assert len(values) == len(COLUMNS_RIB)
        bound = self.prep_stmt_insert_rib.bind(values)
        session.execute(bound)
    
    def insert_updates(self, values):
        """ Insert a line of Updates data into the database.
        :param values: A list containing the values to be inserted.
        """
        assert len(values) == len(COLUMNS_BGPEVENTS)
        bound = self.prep_stmt_insert_bgpevents.bind(values)
        session.execute(bound)
    
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
                'INSERT INTO {0} ({1}) VALUES (?, ?, ?) IF NOT EXISTS'.format(
                    tablename, ",".join(COLUMNS_META)
                ))
            bound = prep_stmt.bind([time.now() * 1000, self.who, original_name])
        else:
            prep_stmt = self.session.prepare(
                'DELETE FROM {0} WHERE {1}=?'.format(tablename, COLUMNS_META[2])
                )
            bound = prep_stmt.bind([original_name])
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