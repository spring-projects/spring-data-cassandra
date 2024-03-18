CREATE KEYSPACE IF NOT EXISTS validation_keyspace WITH durable_writes = false AND replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
USE validation_keyspace;
CREATE TABLE should_pass(id UUID PRIMARY KEY, name TEXT, some_type TEXT, status INT, precision INT);
CREATE TABLE should_fail(id UUID PRIMARY KEY, name INT, type TEXT, status TEXT, precision INT);
