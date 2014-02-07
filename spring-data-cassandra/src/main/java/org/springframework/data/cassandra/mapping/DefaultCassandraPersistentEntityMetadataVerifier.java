/*
 * Copyright 2011-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.MappingException;

/**
 * Default implementation for Cassandra Persistent Entity Verification. Ensures that annotated Persistent Entities will
 * map properly to a Cassandra Table.
 * 
 * @author Matthew T Adams
 * @author David Webb
 */
public class DefaultCassandraPersistentEntityMetadataVerifier implements CassandraPersistentEntityMetadataVerifier {

	@Override
	public void verify(CassandraPersistentEntity<?> entity) throws MappingException {

		boolean todo = true;
		if (todo) {
			return;
		}

		// TODO
		final List<CassandraPersistentProperty> idProperties = new ArrayList<CassandraPersistentProperty>();
		final List<CassandraPersistentProperty> compositePrimaryKeys = new ArrayList<CassandraPersistentProperty>();
		final List<CassandraPersistentProperty> primaryKeyColumns = new ArrayList<CassandraPersistentProperty>();

		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(CassandraPersistentProperty p) {

				if (p.isIdProperty()) {
					idProperties.add(p);
				} else if (p.isCompositePrimaryKey()) {
					compositePrimaryKeys.add(p);
				} else if (p.isPrimaryKeyColumn()) {
					primaryKeyColumns.add(p);
				}

			}
		});

		// TODO Outline Rules needed

		// TODO PK Combinations

		// TODO Index, potential verify against TableMetaData...DO NOT CREATE INDEX.

		// TODO - Uncomment once Matt merges this support
		/*
		if (entity.isCompositePrimaryKey()) {

			if (primaryKeyColumns.size() == 0) {
				throw new IllegalStateException(String.format(
						"composite primary key type [%s] has no fields annotated with @%s", entity.getType().getName(),
						PrimaryKeyColumn.class.getSimpleName()));
			}

			// there can also be no @PrimaryKey or @Id fields that aren't composite primary keys themselves
			for (CassandraPersistentProperty p : idProperties) {
				if (!p.getType().isAnnotationPresent(PrimaryKeyClass.class)) {
					throw new IllegalStateException(String.format(
							"composite primary key type [%s] property [%s] can only be a composite primary key type itself", entity
									.getType().getName(), p.getName()));
				}
			}

			return;
		}

		// else it's not a composite primary key class

		}*/

	}

}
