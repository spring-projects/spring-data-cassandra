/*
 * Copyright 2013-2014 the original author or authors
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

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.annotation.Persistent;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.MappingException;

/**
 * Default implementation for Cassandra Persistent Entity Verification. Ensures that annotated Persistent Entities will
 * map properly to a Cassandra Table.
 *
 * @author Matthew T Adams
 * @author David Webb
 * @author John Blum
 */
public class BasicCassandraPersistentEntityMetadataVerifier implements CassandraPersistentEntityMetadataVerifier {

	private static final Logger log = LoggerFactory.getLogger(BasicCassandraPersistentEntityMetadataVerifier.class);

	protected boolean strict = false;

	@Override
	@SuppressWarnings("all")
	public void verify(CassandraPersistentEntity<?> entity) throws MappingException {

		if (entity.getType().isInterface()){
			return;
		}

		VerifierMappingExceptions exceptions = new VerifierMappingExceptions(entity, String.format(
			"Mapping Exceptions from BasicCassandraPersistentEntityMetadataVerifier for %s", entity.getName()));

		final List<CassandraPersistentProperty> idProperties = new ArrayList<CassandraPersistentProperty>();
		final List<CassandraPersistentProperty> compositePrimaryKeys = new ArrayList<CassandraPersistentProperty>();
		final List<CassandraPersistentProperty> partitionKeyColumns = new ArrayList<CassandraPersistentProperty>();
		final List<CassandraPersistentProperty> clusterKeyColumns = new ArrayList<CassandraPersistentProperty>();
		final List<CassandraPersistentProperty> primaryKeyColumns = new ArrayList<CassandraPersistentProperty>();

		Class<?> entityType = entity.getType();

		boolean isTable = (entityType.isAnnotationPresent(Table.class)
			|| entityType.isAnnotationPresent(Persistent.class));

		boolean isPrimaryKeyClass = entityType.isAnnotationPresent(PrimaryKeyClass.class);

		// Ensure entity is not both a @Table(@Persistent) and a @PrimaryKey
		if (isTable && isPrimaryKeyClass) {
			exceptions.add(new MappingException("Entity cannot be of type Table and PrimaryKey"));
			throw exceptions;
		}

		// Ensure entity is either a @Table/@Persistent or a @PrimaryKey
		if (!isTable && !isPrimaryKeyClass) {
			exceptions.add(new MappingException(
				"Cassandra entities must have the @Table, @Persistent or @PrimaryKeyClass Annotation"));
			throw exceptions;
		}

		// Parse entity properties
		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(CassandraPersistentProperty property) {
				if (property.isIdProperty()) {
					idProperties.add(property);
				} else if (property.isClusterKeyColumn()) {
					clusterKeyColumns.add(property);
					primaryKeyColumns.add(property);
				} else if (property.isCompositePrimaryKey()) {
					compositePrimaryKeys.add(property);
				} else if (property.isPartitionKeyColumn()) {
					partitionKeyColumns.add(property);
					primaryKeyColumns.add(property);
				}
			}
		});

		final int idPropertyCount = idProperties.size();
		final int partitionKeyColumnCount = partitionKeyColumns.size();
		final int primaryKeyColumnCount = primaryKeyColumns.size();

		// Perform rules verification on PrimaryKeyClass
		if (isPrimaryKeyClass) {

			// Must have at least 1 attribute annotated with @PrimaryKeyColumn
			if (primaryKeyColumnCount == 0) {
				exceptions.add(new MappingException(String.format(
					"Composite primary key type [%s] has no fields annotated with @%s", entity.getType().getName(),
						PrimaryKeyColumn.class.getSimpleName())));
			}

			// At least one of the PrimaryKeyColumns must have a type PARTIONED
			if (partitionKeyColumnCount == 0) {
				exceptions.add(new MappingException(
					"At least one of the @PrimaryKeyColumn annotations must have a type of PARTITIONED"));
			}

			// Cannot have any Id or PrimaryKey Annotations
			if (idPropertyCount > 0) {
				exceptions.add(new MappingException(
					"Annotations @Id and @PrimaryKey are invalid for type annotated with @PrimaryKeyClass"));
			}

			// Ensure that PrimaryKeyColumn is a supported Type.
			for (CassandraPersistentProperty property : primaryKeyColumns) {
				if (CassandraSimpleTypeHolder.getDataTypeFor(property.getType()) == null) {
					exceptions.add(new MappingException(
						"Fields annotated with @PrimaryKeyColumn must be simple CassandraTypes"));
				}
			}

			// Ensure PrimaryKeyClass is Serializable
			if (!Serializable.class.isAssignableFrom(entityType)) {
				exceptions.add(new MappingException("@PrimaryKeyClass must be Serializable"));
			}

			// Ensure PrimaryKeyClass only extends Object
			if (!entityType.getSuperclass().equals(Object.class)) {
				exceptions.add(new MappingException("@PrimaryKeyClass must only extend Object"));
			}

			// Check that PrimaryKeyClass overrides "boolean equals(Object)"
			verifyMethodPresent(entityType, "equals", "boolean equals(Object)", exceptions);

			// Ensure PrimaryKeyClass overrides "int hashCode()"
			verifyMethodPresent(entityType, "hashCode", "int hashCode()", exceptions);
		}

		/*
		 * Perform rules verification on Table/Persistent
		 */
		if (isTable) {

			// TODO Verify annotation values with CqlIndentifier

			// Ensure only one PK or at least one partitioned PK Column and not both PK(s) & PK Column(s) exist
			if (primaryKeyColumnCount == 0) {
				// Can only have one PK
				if (idPropertyCount != 1) {
					exceptions.add(new MappingException(String.format(
						"@Table/@Persistent types must have only one @PrimaryKey attribute, if any; Found %s",
						idPropertyCount)));

					throw exceptions;
				}

				// Ensure that Id is a supported Type.  At this point there is only 1.
				Class<?> idType = idProperties.get(0).getType();

				if (!idType.isAnnotationPresent(PrimaryKeyClass.class)
						&& CassandraSimpleTypeHolder.getDataTypeFor(idType) == null) {

					exceptions.add(new MappingException(
						"Fields annotated with @PrimaryKey must be simple CassandraTypes or @PrimaryKeyClass type"));
				}
			} else if (idPropertyCount > 0) {
				// Then we have both PK(s) & PK Column(s)
				exceptions.add(new MappingException(String.format(
					"@Table/@Persistent types must not define both @PrimaryKeyColumn field(s) (found %s) and @PrimaryKey field(s) (found %s)",
					primaryKeyColumnCount, idPropertyCount)));

				throw exceptions;
			} else {
				// We have no PKs & only PK Column(s); ensure at least one is of type PARTITIONED
				if (partitionKeyColumnCount == 0) {
					exceptions.add(new MappingException(String.format(
						"@Table/@Persistent types must define at least one @PrimaryKeyColumn of type PARTITIONED")));
				}
			}
		}

		// Determine whether or not to throw Exception based on errors found
		if (exceptions.getCount() > 0) {
			log.error("Exceptions while verifying PersistentEntity", exceptions);
			throw exceptions;
		}
	}

	boolean verifyMethodPresent(Class<?> type, String methodName, String methodDescription,
			VerifierMappingExceptions exceptions) {
		try {
			Method method = type.getDeclaredMethod(methodName, Object.class);

			if (method == null || !method.getDeclaringClass().equals(type)) {
				throw new NoSuchMethodException();
			}

			return true;
		} catch (NoSuchMethodException e) {
			String message = String.format(
				"@PrimaryKeyClass should override '%s' method and use all @PrimaryKeyColumn fields",
					methodDescription);

			if (strict) {
				exceptions.add(new MappingException(message, e));
			} else {
				log.warn(message);
			}

			return false;
		}
	}

	/**
	 * @return the setting for strict.
	 */
	@SuppressWarnings("unused")
	public boolean isStrict() {
		return strict;
	}

	/**
	 * @param strict boolean setting for strict.
	 */
	@SuppressWarnings("unused")
	public void setStrict(boolean strict) {
		this.strict = strict;
	}
}
