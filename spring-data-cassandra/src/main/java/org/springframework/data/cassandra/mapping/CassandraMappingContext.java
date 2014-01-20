package org.springframework.data.cassandra.mapping;

import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.data.mapping.context.MappingContext;

import com.datastax.driver.core.TableMetadata;

/**
 * A {@link MappingContext} for Cassandra.
 * 
 * @author Matthew T. Adams
 */
public interface CassandraMappingContext extends
		MappingContext<CassandraPersistentEntity<?>, CassandraPersistentProperty> {

	/**
	 * Returns a {@link CreateTableSpecification} for the given entity, including all mapping information.
	 * 
	 * @param The entity. May not be null.
	 */
	CreateTableSpecification getCreateTableSpecificationFor(CassandraPersistentEntity<?> entity);

	/**
	 * Returns whether this mapping context has any entities mapped to the given table.
	 * 
	 * @param table May not be null.
	 */
	boolean usesTable(TableMetadata table);
}
