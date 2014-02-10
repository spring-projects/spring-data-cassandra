package org.springframework.data.cassandra.test.integration.composites;

import org.springframework.data.cassandra.repository.CassandraRepository;

public interface CommentRepository extends CassandraRepository<Comment, CommentKey> {
}
