# rv_ingest

Automatically downloads data from [the Routeview Project](http://archive.routeviews.org/route-views6/bgpdata/) and inserts it into a Cassandra database.

# Dependencies

pycurl - for retrieving files from the routeview server
beautifulsoup4 - for parsing HTML directories
pytz - for timezone information
mrtparse - for parsing the MRT files