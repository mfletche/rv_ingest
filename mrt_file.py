""" Represents each file type present on the routeview archive and contains
functions to INSERT them into a Cassandra db appropriately.
"""

from mrtparse import *
import time
import os

# This will be used as the value for the 'who' field
username = 'marianne'

class ProcessedDataFile(Base):
    """ The base class for specific types of MRT file.
    
    :param inputpath: A string, the path to the unprocessed MRT file.
    :param outputpath: A string, the path to the processed output data file.
    :param delimiter: A string, the delimiter between data columns in the
        output file
    :param session: A cassandra.cluster.Session object, the database session to
        use when inserting items.
    """
    def __init__(self, inputpath, outputpath, delimiter='|', session=None):
        assert inputpath
        assert outputpath
        
        self.name = os.path.basename(inputpath)
        self.outputfile = open(outputpath, 'w')
        self.delimiter = delimiter
        self.session = session
        
    def write_line(self):
        """ Write a line of data from the input MRT file to the output file.
        """
        pass
    
    def is_file_inserted(self):
        """ True if a file with the same name has already been inserted into
        the database.
        """
        assert self.import_query    # Only defined in subclasses
        assert self.session
        
        rows = self.session.execute(self.import_query)
        return (len(rows.current_rows) > 0)
        
    def set_file_inserted(self, inserted):
        """ Determine whether the database has imported the current file.
        """
        
        # TODO: Add case when inserted=False
        assert self.session
        global username
        import_insert = self.import_insert.format(int(time.time()) * 1000, username, self.name)
        result = self.session.execute(import_insert)
    
    def insert_into_db():
        """ Insert the data at the current output file into the database.
        """
        assert self.session
        assert self._do_bulk_insert  # Only defined in subclasses
        
        # Check whether file is already imported
        if not self.is_file_inserted():
            self._do_bulk_insert()
            self.set_file_inserted(True)   # Mark file as imported
        
    def __del__(self):
        self.outputfile.close()
        

class ProcessedRIBFile(ProcessedDataFile):
    """ Represents a RIB file. Can process it into a '|'-delimited list of
    rows.
    """
    
    # Schema for cassandra-loader
    schema = '"bgp6.rib(prefix,peer,peerip,snapshot,ts,aspath)"'
    
    def __init__(self, inputpath, outputpath, session=None):
        ProcessedDataFile.__init__(self, inputpath, outputpath, session=session)
        self.import_query = ("SELECT * FROM importedrib WHERE file='%s'" % self.name)
        self.import_insert = "INSERT INTO importedrib (ts, who, file) VALUES ({0}, '{1}', '{2}')"
        
    def write_line(self):
        self.delimiter.join(
            '"%s"' % self.prefix,
            self.peer_as,
            '"%s"' % self.peer_ip,
            self.snapshot * 1000,
            int(self.ts) * 1000,
            '"%s"' % self.merge_as_path()
        )
        
    def _do_bulk_insert(self):
        pass

class ProcessedUpdatesFile(ProcessedDataFile):
    """ Represents an Updates file. Can process it into a '|'-delimited list of
    rows.
    """
    
    # Schema for cassandra-loader
    schema = '"bgp6.bgpevents(prefix,ts,sequence,peer,peerip,type,aspath)"'
    
    def __init__(self, inputpath, outputpath, session=None):
        ProcessedDataFile.__init__(self, inputpath, outputpath, session=session)
        self.import_query = ("SELECT * FROM imported WHERE file='%s'" % self.name)
        self.import_insert = "INSERT INTO imported (ts, who, file) VALUES ({0}, '{1}', '{2}')"
        
    def write_line(self):
        self.delimiter.join(
            '"%s"' % self.prefix,
            int(self.ts) * 1000,
            self.sn,
            self.peer_as,
            '"%s"' % self.peer_ip,
            '"%s"' % self.flag,
            '"%s"' % self.merge_as_path()
        )
        
    def _do_bulk_insert(self):
        pass