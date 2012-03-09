from pymongo import Connection

# mongodb connection default host and port
_connection = Connection()
_db = _connection.bamboo

@property
def db(): return _db
