package org.springframework.data.cassandra.core.mapping;

import org.springframework.data.mapping.PersistentEntity;

/**
 * 
 * @author Brian O'Neill
 */
public interface CassandraPersistentEntity<T> extends PersistentEntity<T, CassandraPersistentProperty> {

	String getColumnFamily();
}
