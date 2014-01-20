package org.springframework.data.cassandra.mapping;

import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * Interface that stores information about the mapping between a table and the entity or the entities that are mapped to
 * it.
 * 
 * @author Matthew T. Adams
 */
public interface TableMapping {

	/**
	 * Convenience method to return the only member of the set of entity classes. This method can be used when the caller
	 * knows that there is only one entity class in this mapping. If there are multiple and this method is called, an
	 * {@link IllegalStateException} is thrown.
	 */
	@NotNull
	Class<?> getEntityClass();

	/**
	 * Convenience method to set this mapping to use a single entity class.
	 * 
	 * @param entityClass The class; may not be null.
	 */
	void setEntityClass(@NotNull Class<?> entityClass);

	/**
	 * Sets the set of entity classes of this mapping.
	 */
	void setEntityClasses(Set<Class<?>> entityClasses);

	/**
	 * Returns the set of entity classes of this mapping. Never returns null.
	 */
	@NotNull
	Set<Class<?>> getEntityClasses();

	/**
	 * Returns the name of this mapping. Never returns null.
	 */
	@NotNull
	String getTableName();

	/**
	 * Sets the table name of this mapping.
	 */
	void setTableName(@NotNull String tableName);
}
