package org.springframework.data.cassandra.repository.example;

import org.springframework.data.cassandra.repository.support.SimpleCassandraRepository;
import org.springframework.stereotype.Component;

@Component
public class UserRepository extends SimpleCassandraRepository<User, String> {
}
