/*
 * Copyright 2016-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.MappingException;

/**
 * {@link CassandraPersistentEntityMetadataVerifier} for {@link PrimaryKeyClass} entities. Ensures a valid mapping for
 * composite primary keys.
 *
 * @author Mark Paluch
 * @since 1.5
 * @see PrimaryKeyClass
 */
public class PrimaryKeyClassEntityMetadataVerifier implements CassandraPersistentEntityMetadataVerifier {

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.CassandraPersistentEntityMetadataVerifier#verify(org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity)
	 */
	@Override
	public void verify(CassandraPersistentEntity<?> entity) throws MappingException {

		if (entity.getType().isInterface() || !entity.isCompositePrimaryKey()) {
			return;
		}

		List<MappingException> exceptions = new ArrayList<>();

		List<CassandraPersistentProperty> idProperties = new ArrayList<>();
		List<CassandraPersistentProperty> compositePrimaryKeys = new ArrayList<>();
		List<CassandraPersistentProperty> partitionKeyColumns = new ArrayList<>();
		List<CassandraPersistentProperty> primaryKeyColumns = new ArrayList<>();

		Class<?> entityType = entity.getType();

		// @Indexed not allowed on type level
		if (entity.isAnnotationPresent(Indexed.class)) {
			exceptions.add(new MappingException("@Indexed cannot be used on primary key classes"));
		}

		// Ensure entity is not both a @Table(@Persistent) and a @PrimaryKey
		if (entity.isAnnotationPresent(Table.class)) {
			exceptions.add(new MappingException(String.format("Entity cannot be of type @%s and @%s",
					Table.class.getSimpleName(), PrimaryKeyClass.class.getSimpleName())));
		}

		// Ensure PrimaryKeyClass only extends Object
		if (!entityType.getSuperclass().equals(Object.class)) {
			exceptions.add(
					new MappingException(String.format("@%s must only extend Object", PrimaryKeyClass.class.getSimpleName())));
		}

		entity.forEach(property -> {
			if (property.isCompositePrimaryKey()) {
				compositePrimaryKeys.add(property);
			} else if (property.isIdProperty()) {
				idProperties.add(property);
			} else if (property.isClusterKeyColumn()) {
				primaryKeyColumns.add(property);
			} else if (property.isPartitionKeyColumn()) {
				partitionKeyColumns.add(property);
				primaryKeyColumns.add(property);
			}
		});

		if (!compositePrimaryKeys.isEmpty()) {
			exceptions
					.add(new MappingException("Composite primary keys are not allowed inside of composite primary key classes"));
		}

		// Must have at least 1 attribute annotated with @PrimaryKeyColumn
		if (primaryKeyColumns.isEmpty()) {
			exceptions.add(
					new MappingException(String.format("Composite primary key type [%1$s] has no fields annotated with @%2$s",
							entity.getType().getName(), PrimaryKeyColumn.class.getSimpleName())));
		}

		// At least one of the PrimaryKeyColumns must have a type PARTIONED
		if (partitionKeyColumns.isEmpty()) {
			exceptions
					.add(new MappingException(String.format("At least one of the @%s annotations must have a type of PARTITIONED",
							PrimaryKeyColumn.class.getSimpleName())));
		}

		// Cannot have any Id or PrimaryKey Annotations
		if (!idProperties.isEmpty()) {
			exceptions.add(
					new MappingException(String.format("Annotations @%1$s and @%2$s are invalid for type annotated with @%3$s",
							Id.class.getSimpleName(), PrimaryKey.class.getSimpleName(), PrimaryKeyClass.class.getSimpleName())));
		}

		// Determine whether or not to throw Exception based on errors found
		if (!exceptions.isEmpty()) {
			fail(entity, exceptions);
		}
	}

	private static void fail(CassandraPersistentEntity<?> entity, List<MappingException> exceptions) {
		throw new VerifierMappingExceptions(entity, exceptions);
	}
}
