package org.springframework.data.cassandra.mapping;

import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.util.TypeInformation;

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

	/**
	 * Returns the {@link CassandraPersistentEntity} for the given type. If it doesn't exist, this method throws
	 * {@link IllegalArgumentException}.
	 * 
	 * @param type The Java type of the persistent entity.
	 * @return The {@link CassandraPersistentEntity} describing the persistent Java type.
	 * @throws IllegalArgumentException if the persistent entity is unknown
	 */
	public CassandraPersistentEntity<?> getRequiredPersistentEntity(Class<?> type);

	/**
	 * Returns the {@link CassandraPersistentEntity} for the given type. If it doesn't exist, this method throws
	 * {@link IllegalArgumentException}.
	 * 
	 * @param type The {@link TypeInformation} of the persistent entity.
	 * @return The {@link CassandraPersistentEntity} describing the persistent Java type.
	 * @throws IllegalArgumentException if the persistent entity is unknown
	 */
	public CassandraPersistentEntity<?> getRequiredPersistentEntity(TypeInformation<?> type);
}
