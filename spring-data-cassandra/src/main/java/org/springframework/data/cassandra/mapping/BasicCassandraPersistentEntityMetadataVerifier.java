/*
 * Copyright 2013-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.mapping;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.model.MappingException;

/**
 * Default implementation for Cassandra Persistent Entity Verification. Ensures that annotated
 * {@link CassandraPersistentEntity entities} will map properly to a Cassandra Table.
 *
 * @author Matthew T Adams
 * @author David Webb
 * @author John Blum
 * @author Mark Paluch
 * @see Table
 * @see PrimaryKey
 * @see Id
 */
public class BasicCassandraPersistentEntityMetadataVerifier implements CassandraPersistentEntityMetadataVerifier {

	@Deprecated protected boolean strict = false;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.mapping.CassandraPersistentEntityMetadataVerifier#verify(org.springframework.data.cassandra.mapping.CassandraPersistentEntity)
	 */
	@Override
	public void verify(CassandraPersistentEntity<?> entity) throws MappingException {

		if (entity.getType().isInterface() || !entity.findAnnotation(Table.class).isPresent()) {
			return;
		}

		List<MappingException> exceptions = new ArrayList<>();

		final List<CassandraPersistentProperty> idProperties = new ArrayList<>();
		final List<CassandraPersistentProperty> partitionKeyColumns = new ArrayList<>();
		final List<CassandraPersistentProperty> primaryKeyColumns = new ArrayList<>();

		// Ensure entity is not both a @Table(@Persistent) and a @PrimaryKeyClass
		if (entity.isCompositePrimaryKey()) {
			exceptions.add(new MappingException(String.format("Entity cannot be of type @%s and @%s",
					Table.class.getSimpleName(), PrimaryKeyClass.class.getSimpleName())));
		}

		// Parse entity properties
		entity.getPersistentProperties().forEach(property -> {
			if (property.isIdProperty()) {
				idProperties.add(property);
			} else if (property.isClusterKeyColumn()) {
				primaryKeyColumns.add(property);
			} else if (property.isPartitionKeyColumn()) {
				partitionKeyColumns.add(property);
				primaryKeyColumns.add(property);
			}
		});

		// Perform rules verification on Table/Persistent
		// TODO Verify annotation values with CqlIndentifier

		/*
		 * Perform rules verification on Table/Persistent
		 */
		// Ensure only one PK or at least one partitioned PK Column and not both PK(s) & PK Column(s) exist
		if (primaryKeyColumns.isEmpty()) {

			// Can only have one PK
			if (idProperties.size() != 1) {
				exceptions
						.add(new MappingException(String.format("@%s types must have only one primary attribute, if any; Found %s",
								Table.class.getSimpleName(), idProperties.size())));

				fail(entity, exceptions);
			}
		}

		if (!idProperties.isEmpty() && !primaryKeyColumns.isEmpty()) {

			// Then we have both PK(s) & PK Column(s)
			exceptions.add(new MappingException(String.format("@%s types must not define both @%s and @%s properties",
					Table.class.getSimpleName(), Id.class.getSimpleName(), PrimaryKeyColumn.class.getSimpleName())));

			fail(entity, exceptions);
		}

		// We have no PKs & only PK Column(s); ensure at least one is of type PARTITIONED
		if (!primaryKeyColumns.isEmpty() && partitionKeyColumns.isEmpty()) {
			exceptions
					.add(new MappingException(String.format("At least one of the @%s annotations must have a type of PARTITIONED",
							PrimaryKeyColumn.class.getSimpleName())));
		}

		// Determine whether or not to throw Exception based on errors found
		if (!exceptions.isEmpty()) {
			fail(entity, exceptions);
		}
	}

	/**
	 * @return the setting for strict.
	 * @deprecated Will be removed in future versions.
	 */
	@Deprecated
	@SuppressWarnings("unused")
	public boolean isStrict() {
		return strict;
	}

	/**
	 * @param strict boolean setting for strict.
	 * @deprecated Will be removed in future versions.
	 */
	@Deprecated
	@SuppressWarnings("unused")
	public void setStrict(boolean strict) {
		this.strict = strict;
	}

	private static void fail(CassandraPersistentEntity<?> entity, List<MappingException> exceptions) {
		throw new VerifierMappingExceptions(entity, exceptions);
	}
}
