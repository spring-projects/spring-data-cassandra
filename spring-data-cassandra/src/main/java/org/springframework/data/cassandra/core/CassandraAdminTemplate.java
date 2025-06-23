/*
 * Copyright 2013-2025 the original author or authors.
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
package org.springframework.data.cassandra.core;

import java.util.Map;
import java.util.Optional;

import org.springframework.data.cassandra.SessionFactory;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.SchemaFactory;
import org.springframework.data.cassandra.core.cql.CqlOperations;
import org.springframework.data.cassandra.core.cql.SessionCallback;
import org.springframework.data.cassandra.core.cql.generator.CqlGenerator;
import org.springframework.data.cassandra.core.cql.keyspace.CreateTableSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.SpecificationBuilder;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

/**
 * Default implementation of {@link CassandraAdminOperations}.
 *
 * @author Mark Paluch
 * @author Fabio J. Mendes
 * @author John Blum
 * @author Vagif Zeynalov
 * @author Mikhail Polivakha
 * @author Seungho Kang
 */
public class CassandraAdminTemplate extends CassandraTemplate implements CassandraAdminOperations {

	protected static final boolean DEFAULT_DROP_TABLE_IF_EXISTS = false;

	private final SchemaFactory schemaFactory;

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param session must not be {@literal null}.
	 * @since 2.2
	 */
	public CassandraAdminTemplate(CqlSession session) {

		super(session);
		this.schemaFactory = new SchemaFactory(getConverter());
	}

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param session must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 */
	public CassandraAdminTemplate(CqlSession session, CassandraConverter converter) {

		super(session, converter);
		this.schemaFactory = new SchemaFactory(getConverter());
	}

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param sessionFactory must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 */
	public CassandraAdminTemplate(SessionFactory sessionFactory, CassandraConverter converter) {

		super(sessionFactory, converter);
		this.schemaFactory = new SchemaFactory(getConverter());
	}

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param cqlOperations must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @since 2.2
	 */
	public CassandraAdminTemplate(CqlOperations cqlOperations, CassandraConverter converter) {
		super(cqlOperations, converter);

		this.schemaFactory = new SchemaFactory(getConverter());
	}

	@Override
	public SchemaFactory getSchemaFactory() {
		return schemaFactory;
	}

	@Override
	public void createTable(boolean ifNotExists, CqlIdentifier tableName, Class<?> entityClass,
			Map<String, Object> optionsByName) {
		CassandraPersistentEntity<?> entity = getConverter().getMappingContext().getRequiredPersistentEntity(entityClass);

		CreateTableSpecification createTableSpecification = this.schemaFactory
				.getCreateTableSpecificationFor(entity, tableName).ifNotExists(ifNotExists);

		if (!CollectionUtils.isEmpty(optionsByName)) {
			optionsByName.forEach((key, value) -> {
				TableOption tableOption = TableOption.findByNameIgnoreCase(key);
				if (tableOption == null) {
					addRawTableOption(key, value, createTableSpecification);
				} else if (tableOption.requiresValue()) {
					createTableSpecification.with(tableOption, value);
				} else {
					createTableSpecification.with(tableOption);
				}
			});
		}

		getCqlOperations().execute(CqlGenerator.toCql(createTableSpecification));
	}

	private void addRawTableOption(String key, Object value, CreateTableSpecification createTableSpecification) {
		if (value instanceof String) {
			createTableSpecification.with(key, value, true, true);
			return;
		}
		createTableSpecification.with(key, value, false, false);
	}

	@Override
	public void dropTable(Class<?> entityClass) {
		dropTable(getTableName(entityClass));
	}

	@Override
	public void dropTable(CqlIdentifier tableName) {
		dropTable(DEFAULT_DROP_TABLE_IF_EXISTS, tableName);
	}

	@Override
	public void dropTable(boolean ifExists, CqlIdentifier tableName) {

		String dropTableCql = CqlGenerator.toCql(SpecificationBuilder.dropTable(tableName).ifExists(ifExists));

		getCqlOperations().execute(dropTableCql);
	}

	@Override
	public void dropUserType(CqlIdentifier typeName) {

		Assert.notNull(typeName, "Type name must not be null");

		String dropUserTypeCql = CqlGenerator.toCql(SpecificationBuilder.dropType(typeName));

		getCqlOperations().execute(dropUserTypeCql);
	}

	@Override
	public Optional<TableMetadata> getTableMetadata(CqlIdentifier keyspace, CqlIdentifier tableName) {

		Assert.notNull(keyspace, "Keyspace name must not be null");
		Assert.notNull(tableName, "Table name must not be null");

		return getCqlOperations().execute((SessionCallback<Optional<TableMetadata>>) session -> {
			return session.getMetadata().getKeyspace(keyspace).flatMap(it -> it.getTable(tableName));
		});
	}

	@Override
	public KeyspaceMetadata getKeyspaceMetadata() {

		return getCqlOperations().execute((SessionCallback<KeyspaceMetadata>) session -> {

			return session.getKeyspace().flatMap(it -> session.getMetadata().getKeyspace(it)).orElseThrow(() -> {
				return new IllegalStateException("Metadata for keyspace not available");
			});
		});
	}
}
