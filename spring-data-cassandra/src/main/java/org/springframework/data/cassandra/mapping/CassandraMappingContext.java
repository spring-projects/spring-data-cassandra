package org.springframework.data.cassandra.mapping;

import java.util.Collection;

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
	 * Returns only those entities that don't represent primary key types.
	 * 
	 * @see #getPersistentEntities(boolean)
	 */
	@Override
	public Collection<CassandraPersistentEntity<?>> getPersistentEntities();

	/**
	 * Returns all persistent entities or only non-primary-key entities.
	 * 
	 * @param includePrimaryKeyTypes If <code>true</code>, returns all entities, including entities that represent primary
	 *          key types. If <code>false</code>, returns only entities that don't represent primary key types.
	 */
	public Collection<CassandraPersistentEntity<?>> getPersistentEntities(boolean includePrimaryKeyTypes);

	/**
	 * Returns only those entities representing primary key types.
	 */
	Collection<CassandraPersistentEntity<?>> getPrimaryKeyEntities();

	/**
	 * Returns only those entities not representing primary key types.
	 * 
	 * @see #getPersistentEntities(boolean)
	 */
	Collection<CassandraPersistentEntity<?>> getNonPrimaryKeyEntities();

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
