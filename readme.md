Spring Data Cassandra
=====================

This is a Spring Data subproject for Cassandra that uses the binary CQL3 protocol via
the official DataStax 2.0 Java driver (https://github.com/datastax/java-driver).

Supports native CQL3 queries in Spring Repositories.

For now, you can get and start a local Cassandra instance via the \*n\*x script `test-support/get-and-start-cassandra`.
It stores the cassandra process id in the file `.cassandra/dist/dsc-cassandra-x.y.z/cassandra.pid`, where `x.y.z` is the latest version of Cassandra.  You can use this process id to stop Cassandra via ``kill `cat .cassandra/dist/dsc-cassandra-x.y.z/cassandra.pid` ``.