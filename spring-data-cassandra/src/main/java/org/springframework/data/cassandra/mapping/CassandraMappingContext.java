package org.springframework.data.cassandra.mapping;

import java.util.Collection;

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
	 * Returns the existing {@link CassandraPersistentEntity} for the given {@link Class}. If it is not yet known to this
	 * {@link CassandraMappingContext}, an {@link IllegalArgumentException} is thrown.
	 * 
	 * @param type The class of the existing persistent entity.
	 * @return The existing persistent entity.
	 */
	CassandraPersistentEntity<?> getExistingPersistentEntity(Class<?> type);

	/**
	 * Returns whether this {@link CassandraMappingContext} already contains a {@link CassandraPersistentEntity} for the
	 * given type.
	 */
	boolean contains(Class<?> type);

	/**
	 * Sets a verifier other than the {@link DefaultCassandraPersistentEntityMetadataVerifier}
	 */
	void setVerifier(CassandraPersistentEntityMetadataVerifier verifier);
}
