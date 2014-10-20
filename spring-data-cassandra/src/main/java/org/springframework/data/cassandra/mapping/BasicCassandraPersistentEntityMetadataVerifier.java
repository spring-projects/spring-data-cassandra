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
import org.springframework.data.util.TypeInformation;

/**
 * Default implementation for Cassandra Persistent Entity Verification. Ensures that annotated Persistent Entities will
 * map properly to a Cassandra Table.
 * 
 * @author Matthew T Adams
 * @author David Webb
 */
public class BasicCassandraPersistentEntityMetadataVerifier implements CassandraPersistentEntityMetadataVerifier {

	private static final Logger log = LoggerFactory.getLogger(BasicCassandraPersistentEntityMetadataVerifier.class);

	protected boolean strict = false;

	@Override
	public void verify(CassandraPersistentEntity<?> entity) throws MappingException {

		VerifierMappingExceptions exceptions = new VerifierMappingExceptions(entity,
				"Mapping Exceptions from BasicCassandraPersistentEntityMetadataVerifier");

		final List<CassandraPersistentProperty> idProperties = new ArrayList<CassandraPersistentProperty>();
		final List<CassandraPersistentProperty> compositePrimaryKeys = new ArrayList<CassandraPersistentProperty>();
		final List<CassandraPersistentProperty> partitionKeyColumns = new ArrayList<CassandraPersistentProperty>();
		final List<CassandraPersistentProperty> clusterKeyColumns = new ArrayList<CassandraPersistentProperty>();
		final List<CassandraPersistentProperty> primaryKeyColumns = new ArrayList<CassandraPersistentProperty>();

		/*
		 * Determine how this type is annotated
		 */
		Class<?> thisType = entity.getType();

		boolean isTable = isTable(thisType);
		boolean isPrimaryKeyClass = isPrimaryKeyClass(thisType);

		/*
		 * Ensure that this is not both a @Table(@Persistent) and a @PrimaryKey
		 */
		if (isTable && isPrimaryKeyClass) {
			exceptions.add(new MappingException("Entity cannot be of type Table and PrimaryKey"));
			throw exceptions;
		}

		/*
		 * Ensure that this is either a @Table(@Persistent) or a @PrimaryKey
		 */
		if (!isTable && !isPrimaryKeyClass) {
			exceptions.add(new MappingException(
					"Cassandra entities must have the @Table, @Persistent or @PrimaryKeyClass Annotation"));
			throw exceptions;
		}

		/*
		 * Parse the properties
		 */
		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(CassandraPersistentProperty p) {

				if (p.isIdProperty()) {
					idProperties.add(p);
				} else if (p.isCompositePrimaryKey()) {
					compositePrimaryKeys.add(p);
				} else if (p.isPartitionKeyColumn()) {
					partitionKeyColumns.add(p);
					primaryKeyColumns.add(p);
				} else if (p.isClusterKeyColumn()) {
					clusterKeyColumns.add(p);
					primaryKeyColumns.add(p);
				}
			}
		});

		final int idPropertyCount = idProperties.size();
		final int partitionKeyColumnCount = partitionKeyColumns.size();
		final int primaryKeyColumnCount = primaryKeyColumns.size();

		/*
		 * Perform rules verification on PrimaryKeyClass
		 */
		if (isPrimaryKeyClass) {

			/*
			 * Must have at least 1 attribute annotated with @PrimaryKeyColumn
			 */
			if (primaryKeyColumnCount == 0) {
				exceptions.add(new MappingException(String.format(
						"composite primary key type [%s] has no fields annotated with @%s", entity.getType().getName(),
						PrimaryKeyColumn.class.getSimpleName())));
			}

			/*
			 * At least one of the PrimaryKeyColumns must have a type PARTIONED
			 */
			if (partitionKeyColumnCount == 0) {
				exceptions.add(new MappingException(
						"At least one of the @PrimaryKeyColumn annotation must have a type of PARTITIONED"));
			}

			/*
			 * Cannot have any Id or PrimaryKey Annotations
			 */
			if (idPropertyCount > 0) {
				exceptions.add(new MappingException(
						"Annotations @Id and @PrimaryKey are invalid for type annotated with @PrimaryKeyClass"));
			}

			/*
			 * Ensure that PrimaryKeyColumn is a supported Type.
			 */
//			for (CassandraPersistentProperty p : primaryKeyColumns) {
//				if (CassandraSimpleTypeHolder.getDataTypeFor(p.getType()) == null) {
//					exceptions.add(new MappingException("Fields annotated with @PrimaryKeyColumn must be simple CassandraTypes"));
//				}
//			}

			/*
			 * Ensure PrimaryKeyClass is Serializable
			 */
			if (!Serializable.class.isAssignableFrom(thisType)) {
				exceptions.add(new MappingException("@PrimaryKeyClass must be Serializable"));
			}

			/*
			 * Ensure PrimaryKeyClass only extends Object
			 */
			if (!thisType.getSuperclass().equals(Object.class)) {
				exceptions.add(new MappingException("@PrimaryKeyClass must only extend Object"));
			}

			/*
			 * Check that PrimaryKeyClass overrides "boolean equals(Object)"
			 */
			try {
				Method equalsMethod = thisType.getDeclaredMethod("equals", Object.class);
				if (equalsMethod == null || !equalsMethod.getDeclaringClass().equals(thisType)) {
					throw new NoSuchMethodException();
				}
			} catch (NoSuchMethodException e) {
				String message = "@PrimaryKeyClass should override 'boolean equals(Object)' method and use all @PrimaryKeyColumn fields";
				if (strict) {
					exceptions.add(new MappingException(message, e));
				} else {
					log.warn(message);
				}
			}

			/*
			 * Ensure PrimaryKeyClass overrides "int hashCode()"
			 */
			try {
				Method hashCodeMethod = thisType.getDeclaredMethod("hashCode", (Class<?>[]) null);
				if (hashCodeMethod == null || !hashCodeMethod.getDeclaringClass().equals(thisType)) {
					throw new NoSuchMethodException();
				}
			} catch (NoSuchMethodException e) {
				String message = "@PrimaryKeyClass should override 'int hashCode()' method and use all @PrimaryKeyColumn fields";
				if (strict) {
					exceptions.add(new MappingException(message, e));
				} else {
					log.warn(message);
				}
			}
		}

		/*
		 * Perform rules verification on Table/Persistent
		 */
		if (isTable) {

			/*
			 * TODO Verify annotation values with CqlIndentifier
			 */

			/*
			 * Ensure only one PK or at least one partitioned PKC and not both PK(s) & PKC(s)
			 */
			if (primaryKeyColumnCount == 0) {
				/*
				 * Can only have one PK.
				 */
				if (idPropertyCount != 1) {
					exceptions
							.add(new MappingException(String.format(
									"@Table/@Persistent types must have only one @PrimaryKey attribute, if any.  Found %s.",
									idPropertyCount)));
					throw exceptions;
				}
				/*
				 * Ensure that Id is a supported Type.  At the point there is only 1.
				 */
//				Class<?> typeClass = idProperties.get(0).getType();
//				if (!typeClass.isAnnotationPresent(PrimaryKeyClass.class)
//						&& CassandraSimpleTypeHolder.getDataTypeFor(typeClass) == null) {
//					exceptions.add(new MappingException(
//							"Fields annotated with @PrimaryKey must be simple CassandraTypes or @PrimaryKeyClass type"));
//				}
			} else if (idPropertyCount > 0) {
				/*
				 * Then we have both PK(s) & PKC(s)
				 */
				exceptions
						.add(new MappingException(
								String
										.format(
												"@Table/@Persistent types must not define both @PrimaryKeyColumn field%s (found %s) and @PrimaryKey field%s (found %s)",
												primaryKeyColumnCount == 1 ? "" : "s", primaryKeyColumnCount, idPropertyCount == 1 ? "" : "s",
												idPropertyCount)));
				throw exceptions;
			} else {
				/*
				 * We have no PKs & only PKC(s) -- ensure at least one is of type PARTITIONED
				 */
				if (partitionKeyColumnCount == 0) {
					exceptions.add(new MappingException(String
							.format("@Table/@Persistent types must define at least one @PrimaryKeyColumn of type PARTITIONED")));
				}
			}
		}

		/*
		 * Determine whether or not to throw Exception based on errors found
		 */
		if (exceptions.getCount() > 0) {
			log.error("Exceptions while verifying PersistentEntity", exceptions);
			throw exceptions;
		}
	}

    protected boolean isPrimaryKeyClass(Class<?> thisType) {
        return thisType.isAnnotationPresent(PrimaryKeyClass.class);
    }

    protected boolean isTable(Class<?> thisType) {
        return (thisType.isAnnotationPresent(Table.class) || thisType.isAnnotationPresent(Persistent.class));
    }
    
    public boolean isPeristent(Class<?> thisType) {
        return isPrimaryKeyClass(thisType) || isTable(thisType);
    }
    
    @Override
    public boolean isPersistent(TypeInformation<?> info) {
        return isPeristent(info.getType());
    }

	/**
	 * @return Returns the strict.
	 */
	public boolean isStrict() {
		return strict;
	}

	/**
	 * @param strict The strict to set.
	 */
	public void setStrict(boolean strict) {
		this.strict = strict;
	}
}
