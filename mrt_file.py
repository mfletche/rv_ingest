""" Represents each file type present on the routeview archive and contains
functions to INSERT them into a Cassandra db appropriately.
"""

from mrtparse import *
import time
import os

# This will be used as the value for the 'who' field
username = 'marianne'

class MRTFile(Base):
    """ Base file type.
    """
    def __init__(self, inputpath, outputpath, delimiter='|', session=None):
        """ Open input and output paths. Define the delimiter used. Connect to
        a Cassandra session if provided.
        """
        
        assert inputpath
        assert outputpath
        
        self.name = os.path.basename(inputpath)
        self.outputfile = open(outputpath, 'w')
        self.reader = Reader(inputpath)
        self.delimiter = delimiter
        self.session = session
        
    def write_line(self):
        """ This function (in the subclasses) defines how a line is displayed
        in the processed temporary file.
        """
        pass
    
    def is_file_inserted(self):
        """ Check the meta tables in the Cassandra database for files which
        match the current filename. 'import_stmt' is created in the __init__
        function of each subclass.
        """
        assert self.import_query
        assert self.session
        
        rows = self.session.execute(self.import_query)
        return (len(rows.current_rows) > 0)
        
    def insert_into_meta_table(self):
        """ Insert a row into the meta table in the Cassandra database for this
        file. 'import_insert' is defined in each subclass.
        """
        assert self.session
        global username
        self.import_insert = ("INSERT INTO importedrib (ts, who, file) VALUES (%s, '%s', '%s')"
            % (int(time.time()) * 1000, username, self.name))
        result = self.session.execute(import_insert)
    
    def do_bulk_insert():
        assert outputfile
    
    def insert_into_db():
        pass
        
    def __del__(self):
        self.inputfile.close()
        self.output#loader_args = ['-f', '%s' % (tmpname), '-host', '130.217.250.114', '-schema', '%s' % (rib_schema)]file.close()
        

class RIBFile(MRTFile):
    """ Represents a RIB file. Can process it into a '|'-delimited list of
    rows.
    """
    
    # Schema for cassandra-loader
    schema = '"bgp6.rib(prefix,peer,peerip,snapshot,ts,aspath)"'
    
    def __init__(self, inputpath, outputpath, session=None):
        MRTFile.__init__(self, inputpath, outputpath, session=session)
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
        
    def insert_into_meta_table(self):
        assert self.session
        global username
        
        # Substitute values in each time because timestamp may change.
        import_insert = self.import_insert.format(int(time.time()) * 1000, username, self.name)
        result = self.session.execute(import_insert)
        
    def insert_into_db(self):
        assert self.session
        
        if not self.is_file_inserted():
            self.do_bulk_insert()
            
        # Update "meta" table with filename
        else:
            raise

class UpdatesFile(MRTFile):
    """ Represents an Updates file. Can process it into a '|'-delimited list of
    rows.
    """
    
    # Schema for cassandra-loader
    schema = '"bgp6.bgpevents(prefix,ts,sequence,peer,peerip,type,aspath)"'
    
    def __init__(self, inputpath, outputpath, session=None):
        MRTFile.__init__(self, inputpath, outputpath, session=session)
        self.import_query = ("SELECT * FROM imported WHERE file='%s'" % self.name)
        self.import_insert = ("INSERT INTO imported (ts, who, file) VALUES ({0}, '{1}', '{2}')"
        
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
    
    def insert_into_meta_table(self):
        assert self.session
        global username
        import_insert = self.import_insert.format(int(time.time()) * 1000, username, self.name)
        result = self.session.execute(import_insert)
    
    def insert_into_db(self):
        assert self.session
        
        # Check if file is in db
        # Bulk insert
        # Update "meta" table with filename