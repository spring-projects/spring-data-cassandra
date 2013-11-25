To configure Cassandra, run the following through CLI:
create keyspace KunderaExamples;
use KunderaExamples;
create column family users with comparator=UTF8Type and default_validation_class=UTF8Type and key_validation_class=UTF8Type;
