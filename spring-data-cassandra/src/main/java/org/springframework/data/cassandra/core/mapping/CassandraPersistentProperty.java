/*
 * Copyright 2013-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedType;

import org.jspecify.annotations.Nullable;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Cassandra specific {@link org.springframework.data.mapping.PersistentProperty} extension.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author David T. Webb
 * @author Mark Paluch
 * @author John Blum
 * @author Christoph Strobl
 * @author Frank Spitulski
 * @author Aleksei Zotov
 */
public interface CassandraPersistentProperty
		extends PersistentProperty<CassandraPersistentProperty>, ApplicationContextAware {

	/**
	 * If this property is mapped with a single column, set the column name to the given {@link CqlIdentifier}. If this
	 * property is not mapped by a single column, throws {@link IllegalStateException}. If the given column name is null,
	 * {@link IllegalArgumentException} is thrown.
	 *
	 * @param columnName must not be {@literal null}.
	 */
	void setColumnName(CqlIdentifier columnName);

	/**
	 * The name of the single column to which the property is persisted.
	 */
	@Nullable
	CqlIdentifier getColumnName();

	/**
	 * The name of the single column to which the property is persisted.
	 *
	 * @throws IllegalStateException if the required column name is not available.
	 * @since 2.1
	 */
	default CqlIdentifier getRequiredColumnName() {

		CqlIdentifier columnName = getColumnName();

		Assert.state(columnName != null, () -> String
				.format("No column name available for this persistent property [%1$s.%2$s]", getOwner().getName(), getName()));

		return columnName;
	}

	/**
	 * Return whether the property has an explicitly configured column name. Eg. via {@link Column#value()},
	 * {@link PrimaryKey#value()} or {@link PrimaryKeyColumn#name()}
	 *
	 * @return {@literal true} if a configured column name is present and non empty.
	 * @since 3.4
	 */
	boolean hasExplicitColumnName();

	/**
	 * The name of the element ordinal to which the property is persisted when the owning type is a mapped tuple.
	 */
	@Nullable
	Integer getOrdinal();

	/**
	 * The required element ordinal to which the property is persisted when the owning type is a mapped tuple.
	 *
	 * @throws IllegalStateException if the required ordinal is not available.
	 * @since 2.1
	 */
	default int getRequiredOrdinal() {

		Integer ordinal = getOrdinal();

		Assert.state(ordinal != null,
			() -> String.format("No ordinal available for this persistent property [%1$s.%2$s]",
				getOwner().getName(), getName()));

		return ordinal;
	}

	/**
	 * Determines whether this {@link CassandraPersistentProperty} is persisted (mapped) to an element ordinal when the
	 * owning type is a mapped tuple.
	 *
	 * @return a boolean value indicating whether this {@link CassandraPersistentProperty} is persisted (mapped) to an
	 *         element ordinal when the owning type is a mapped tuple.
	 * @see #getOrdinal()
	 * @since 4.0
	 */
	default boolean hasOrdinal() {
		return getOrdinal() != null;
	}

	/**
	 * The ordering (ascending or descending) for the column. Valid only for primary key columns; returns null for
	 * non-primary key columns.
	 */
	@Nullable
	Ordering getPrimaryKeyOrdering();

	/**
	 * Whether the property is a cluster key column.
	 */
	boolean isClusterKeyColumn();

	/**
	 * Whether the property is a composite primary key.
	 */
	boolean isCompositePrimaryKey();

	/**
	 * Returns whether the property is a {@link java.util.Map}.
	 *
	 * @return a boolean indicating whether this property type is a {@link java.util.Map}.
	 */
	boolean isMapLike();

	/**
	 * Whether the property is a partition key column.
	 */
	boolean isPartitionKeyColumn();

	/**
	 * Whether the property is a partition key column or a cluster key column
	 *
	 * @see #isPartitionKeyColumn()
	 * @see #isClusterKeyColumn()
	 */
	boolean isPrimaryKeyColumn();

	/**
	 * Whether the property maps to a static column.
	 *
	 * @since 3.2
	 */
	boolean isStaticColumn();

	/**
	 * @return {@literal true} if the property should be embedded.
	 * @since 3.0
	 */
	default boolean isEmbedded() {
		return findAnnotation(Embedded.class) != null && isEntity();
	}

	/**
	 * Find an {@link AnnotatedType} by {@code annotationType} derived from the property type. Annotated type is looked up
	 * by introspecting property field/accessors. Collection/Map-like types are introspected for type annotations within
	 * type arguments.
	 *
	 * @param annotationType must not be {@literal null}.
	 * @return the annotated type or {@literal null}.
	 * @since 2.0
	 */
	@Nullable
	AnnotatedType findAnnotatedType(Class<? extends Annotation> annotationType);

}
