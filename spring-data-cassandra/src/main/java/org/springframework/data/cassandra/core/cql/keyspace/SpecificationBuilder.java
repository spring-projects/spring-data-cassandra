/*
 * Copyright 2024-present the original author or authors.
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
package org.springframework.data.cassandra.core.cql.keyspace;

import org.jspecify.annotations.Nullable;

import org.springframework.util.ObjectUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;

/**
 * Entrypoint to create CQL specifications to add, alter and delete CQL objects such as tables and index.
 * <p>
 * Specifications can be rendered to CQL using
 * {@link org.springframework.data.cassandra.core.cql.generator.CqlGenerator#toCql(CqlSpecification)} for direct usage
 * with the database.
 *
 * @author Mark Paluch
 * @since 4.4
 * @see org.springframework.data.cassandra.core.cql.generator.CqlGenerator
 */
public final class SpecificationBuilder {

	private SpecificationBuilder() {
		// utility class, no instances
	}

	// -------------------------------------------------------------------------
	// Methods dealing with keyspace creation, alteration, and deletion.
	// -------------------------------------------------------------------------

	/**
	 * Entry point into the {@link CreateKeyspaceSpecification}'s fluent API given {@code name} to create a keyspace.
	 * Convenient if imported statically.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @return a new {@link CreateKeyspaceSpecification}.
	 */
	public static CreateKeyspaceSpecification createKeyspace(String name) {
		return CreateKeyspaceSpecification.createKeyspace(name);
	}

	/**
	 * Entry point into the {@link CreateKeyspaceSpecification}'s fluent API given {@code name} to create a keyspace.
	 * Convenient if imported statically.
	 *
	 * @param name must not be {@literal null}.
	 * @return a new {@link CreateKeyspaceSpecification}.
	 */
	public static CreateKeyspaceSpecification createKeyspace(CqlIdentifier name) {
		return CreateKeyspaceSpecification.createKeyspace(name);
	}

	/**
	 * Entry point into the {@link AlterKeyspaceSpecification}'s fluent API given {@code name} to alter a keyspace.
	 * Convenient if imported statically.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @return a new {@link AlterKeyspaceSpecification}.
	 */
	public static AlterKeyspaceSpecification alterKeyspace(String name) {
		return AlterKeyspaceSpecification.alterKeyspace(name);
	}

	/**
	 * Entry point into the {@link AlterKeyspaceSpecification}'s fluent API given {@code name} to alter a keyspace.
	 * Convenient if imported statically.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @return a new {@link AlterKeyspaceSpecification}.
	 */
	public static AlterKeyspaceSpecification alterKeyspace(CqlIdentifier name) {
		return AlterKeyspaceSpecification.alterKeyspace(name);
	}

	/**
	 * Create a new {@link DropKeyspaceSpecification} for the given {@code name}.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @return a new {@link DropKeyspaceSpecification}.
	 */
	public static DropKeyspaceSpecification dropKeyspace(String name) {
		return DropKeyspaceSpecification.dropKeyspace(name);
	}

	/**
	 * Create a new {@link DropKeyspaceSpecification} for the given {@code name}.
	 *
	 * @param name must not be {@literal null}.
	 * @return a new {@link DropKeyspaceSpecification}.
	 */
	public static DropKeyspaceSpecification dropKeyspace(CqlIdentifier name) {
		return DropKeyspaceSpecification.dropKeyspace(name);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with table creation, alteration, and deletion.
	// -------------------------------------------------------------------------

	/**
	 * Entry point into the {@link CreateTableSpecification}'s fluent API given {@code tableName} to create a table.
	 * Convenient if imported statically.
	 *
	 * @param tableName must not be {@literal null} or empty.
	 * @return a new {@link CreateTableSpecification}.
	 */
	public static CreateTableSpecification createTable(String tableName) {
		return CreateTableSpecification.createTable(tableName);
	}

	/**
	 * Entry point into the {@link CreateTableSpecification}'s fluent API given {@code tableName} to create a table.
	 * Convenient if imported statically. Uses the default keyspace if {@code keyspace} is null; otherwise, of the
	 * {@code keyspace} is not {@literal null}, then the table name is prefixed with {@code keyspace}.
	 *
	 * @param keyspace can be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return a new {@link CreateTableSpecification}.
	 */
	public static CreateTableSpecification createTable(@Nullable String keyspace, String tableName) {
		return createTable(getOptionalKeyspace(keyspace), CqlIdentifier.fromCql(tableName));
	}

	/**
	 * Entry point into the {@link CreateTableSpecification}'s fluent API given {@code tableName} to create a table.
	 * Convenient if imported statically.
	 *
	 * @param tableName must not be {@literal null}.
	 * @return a new {@link CreateTableSpecification}.
	 */
	public static CreateTableSpecification createTable(CqlIdentifier tableName) {
		return CreateTableSpecification.createTable(tableName);
	}

	/**
	 * Entry point into the {@link CreateTableSpecification}'s fluent API given {@code tableName} to create a table.
	 * Convenient if imported statically. Uses the default keyspace if {@code keyspace} is null; otherwise, of the
	 * {@code keyspace} is not {@literal null}, then the table name is prefixed with {@code keyspace}.
	 *
	 * @param keyspace can be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return a new {@link CreateTableSpecification}.
	 */
	public static CreateTableSpecification createTable(@Nullable CqlIdentifier keyspace, CqlIdentifier tableName) {
		return CreateTableSpecification.createTable(keyspace, tableName);
	}

	/**
	 * Entry point into the {@link AlterTableSpecification}'s fluent API given {@code tableName} to alter a table.
	 * Convenient if imported statically.
	 *
	 * @param tableName must not be {@literal null} or empty.
	 * @return a new {@link AlterTableSpecification}.
	 */
	public static AlterTableSpecification alterTable(String tableName) {
		return AlterTableSpecification.alterTable(tableName);
	}

	/**
	 * Entry point into the {@link AlterTableSpecification}'s fluent API given {@code tableName} to alter a table.
	 * Convenient if imported statically. Uses the default keyspace if {@code keyspace} is null; otherwise, of the
	 * {@code keyspace} is not {@literal null}, then the table name is prefixed with {@code keyspace}.
	 *
	 * @param keyspace can be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return a new {@link AlterTableSpecification}.
	 */
	public static AlterTableSpecification alterTable(@Nullable String keyspace, String tableName) {
		return alterTable(getOptionalKeyspace(keyspace), CqlIdentifier.fromCql(tableName));
	}

	/**
	 * Entry point into the {@link AlterTableSpecification}'s fluent API given {@code tableName} to alter a table.
	 * Convenient if imported statically.
	 *
	 * @param tableName must not be {@literal null}.
	 * @return a new {@link AlterTableSpecification}.
	 */
	public static AlterTableSpecification alterTable(CqlIdentifier tableName) {
		return AlterTableSpecification.alterTable(null, tableName);
	}

	/**
	 * Entry point into the {@link AlterTableSpecification}'s fluent API given {@code tableName} to alter a table.
	 * Convenient if imported statically. Uses the default keyspace if {@code keyspace} is null; otherwise, of the
	 * {@code keyspace} is not {@literal null}, then the table name is prefixed with {@code keyspace}.
	 *
	 * @param keyspace can be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return a new {@link AlterTableSpecification}.
	 */
	public static AlterTableSpecification alterTable(@Nullable CqlIdentifier keyspace, CqlIdentifier tableName) {
		return AlterTableSpecification.alterTable(keyspace, tableName);
	}

	/**
	 * Entry point into the {@link DropTableSpecification}'s fluent API {@code tableName} to drop a table. Convenient if
	 * imported statically.
	 *
	 * @param tableName must not be {@literal null} or empty.
	 * @return a new {@link DropTableSpecification}.
	 */
	public static DropTableSpecification dropTable(String tableName) {
		return DropTableSpecification.dropTable(tableName);
	}

	/**
	 * Entry point into the {@link DropTableSpecification}'s fluent API given {@code tableName} to drop a table.
	 * Convenient if imported statically. Uses the default keyspace if {@code keyspace} is null; otherwise, of the
	 * {@code keyspace} is not {@literal null}, then the table name is prefixed with {@code keyspace}.
	 *
	 * @param keyspace can be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return a new {@link DropTableSpecification}.
	 */
	public static DropTableSpecification dropTable(@Nullable String keyspace, String tableName) {
		return dropTable(getOptionalKeyspace(keyspace), CqlIdentifier.fromCql(tableName));
	}

	/**
	 * Entry point into the {@link DropTableSpecification}'s fluent API given {@code tableName} to drop a table.
	 * Convenient if imported statically.
	 *
	 * @param tableName must not be {@literal null}.
	 * @return a new {@link DropTableSpecification}.
	 */
	public static DropTableSpecification dropTable(CqlIdentifier tableName) {
		return DropTableSpecification.dropTable(tableName);
	}

	/**
	 * Entry point into the {@link DropTableSpecification}'s fluent API given {@code tableName} to drop a table.
	 * Convenient if imported statically. Uses the default keyspace if {@code keyspace} is null; otherwise, of the
	 * {@code keyspace} is not {@literal null}, then the table name is prefixed with {@code keyspace}.
	 *
	 * @param keyspace can be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return a new {@link DropTableSpecification}.
	 */
	public static DropTableSpecification dropTable(@Nullable CqlIdentifier keyspace, CqlIdentifier tableName) {
		return DropTableSpecification.dropTable(keyspace, tableName);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with table column alteration.
	// -------------------------------------------------------------------------

	/**
	 * Create a new {@link AddColumnSpecification} for the given {@code name} and {@link DataType}.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @param type must not be {@literal null}.
	 * @return a new {@link AddColumnSpecification}.
	 */
	public static AddColumnSpecification addColumn(String name, DataType type) {
		return AddColumnSpecification.addColumn(name, type);
	}

	/**
	 * Create a new {@link AddColumnSpecification} for the given {@code name} and {@link DataType}.
	 *
	 * @param name must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @return a new {@link AddColumnSpecification}.
	 */
	public static AddColumnSpecification addColumn(CqlIdentifier name, DataType type) {
		return AddColumnSpecification.addColumn(name, type);
	}

	/**
	 * Entry point into the {@link AlterColumnSpecification}'s fluent API given {@code name} and {@link DataType} to alter
	 * a column. Convenient if imported statically.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @param type must not be {@literal null}.
	 * @return a new {@link AlterColumnSpecification}.
	 */
	public static AlterColumnSpecification alterColumn(String name, DataType type) {
		return AlterColumnSpecification.alterColumn(name, type);
	}

	/**
	 * Entry point into the {@link AlterColumnSpecification}'s fluent API given {@code name} and {@link DataType} to alter
	 * a column. Convenient if imported statically.
	 *
	 * @param name must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @return a new {@link AlterColumnSpecification}.
	 */
	public static AlterColumnSpecification alterColumn(CqlIdentifier name, DataType type) {
		return AlterColumnSpecification.alterColumn(name, type);
	}

	/**
	 * Create a new {@link DropColumnSpecification} for the given {@code name}.
	 *
	 * @param name must not be {@literal null} or empty.
	 */
	public static DropColumnSpecification dropColumn(String name) {
		return DropColumnSpecification.dropColumn(name);
	}

	/**
	 * Create a new {@link DropColumnSpecification} for the given {@code name}.
	 *
	 * @param name must not be {@literal null} or empty.
	 */
	public static DropColumnSpecification dropColumn(CqlIdentifier name) {
		return DropColumnSpecification.dropColumn(name);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with index creation and deletion.
	// -------------------------------------------------------------------------

	/**
	 * Entry point into the {@link CreateIndexSpecification}'s fluent API to create an index. Convenient if imported
	 * statically.
	 */
	public static CreateIndexSpecification createIndex() {
		return CreateIndexSpecification.createIndex();
	}

	/**
	 * Entry point into the {@link CreateIndexSpecification}'s fluent API given {@code indexName} to create an index.
	 * Convenient if imported statically.
	 *
	 * @param indexName must not be {@literal null} or empty.
	 * @return a new {@link CreateIndexSpecification}.
	 */
	public static CreateIndexSpecification createIndex(String indexName) {
		return CreateIndexSpecification.createIndex(indexName);
	}

	/**
	 * Entry point into the {@link CreateIndexSpecification}'s fluent API given {@code indexName} to create an index.
	 * Convenient if imported statically.
	 *
	 * @param indexName must not be {@literal null} or empty.
	 * @return a new {@link CreateIndexSpecification}.
	 */
	public static CreateIndexSpecification createIndex(@Nullable String keyspace, String indexName) {
		return createIndex(getOptionalKeyspace(keyspace), CqlIdentifier.fromCql(indexName));
	}

	/**
	 * Entry point into the {@link CreateIndexSpecification}'s fluent API given {@code indexName} to create an index.
	 * Convenient if imported statically.
	 *
	 * @param indexName must not be {@literal null}.
	 * @return a new {@link CreateIndexSpecification}.
	 */
	public static CreateIndexSpecification createIndex(CqlIdentifier indexName) {
		return CreateIndexSpecification.createIndex(null, indexName);
	}

	/**
	 * Entry point into the {@link CreateIndexSpecification}'s fluent API given {@code keyspace} and {@code indexName} to
	 * create an index. Convenient if imported statically. Uses the default keyspace if {@code keyspace} is null;
	 * otherwise, of the {@code keyspace} is not {@literal null}, then the index and table name are prefixed with
	 * {@code keyspace}.
	 *
	 * @param keyspace can be {@literal null}.
	 * @param indexName can be {@literal null}.
	 * @return a new {@link CreateIndexSpecification}.
	 */
	public static CreateIndexSpecification createIndex(@Nullable CqlIdentifier keyspace,
			@Nullable CqlIdentifier indexName) {
		return CreateIndexSpecification.createIndex(keyspace, indexName);
	}

	/**
	 * Create a new {@link DropIndexSpecification} for the given {@code indexName}.
	 *
	 * @param indexName must not be {@literal null} or empty.
	 * @return a new {@link DropIndexSpecification}.
	 */
	public static DropIndexSpecification dropIndex(String indexName) {
		return DropIndexSpecification.dropIndex(indexName);
	}

	/**
	 * Create a new {@link DropIndexSpecification} for the given {@code indexName}. Uses the default keyspace if
	 * {@code keyspace} is null; otherwise, of the {@code keyspace} is not {@literal null}, then the index name is
	 * prefixed with {@code keyspace}.
	 *
	 * @param keyspace can be {@literal null}.
	 * @param indexName must not be {@literal null}.
	 * @return a new {@link DropIndexSpecification}.
	 */
	public static DropIndexSpecification dropIndex(@Nullable String keyspace, String indexName) {
		return dropIndex(getOptionalKeyspace(keyspace), CqlIdentifier.fromCql(indexName));
	}

	/**
	 * Create a new {@link DropIndexSpecification} for the given {@code indexName}.
	 *
	 * @param indexName must not be {@literal null}.
	 * @return a new {@link DropIndexSpecification}.
	 */
	public static DropIndexSpecification dropIndex(CqlIdentifier indexName) {
		return DropIndexSpecification.dropIndex(null, indexName);
	}

	/**
	 * Create a new {@link DropIndexSpecification} for the given {@code indexName}. Uses the default keyspace if
	 * {@code keyspace} is null; otherwise, of the {@code keyspace} is not {@literal null}, then the index name is
	 * prefixed with {@code keyspace}.
	 *
	 * @param keyspace can be {@literal null}.
	 * @param indexName must not be {@literal null}.
	 * @return a new {@link DropIndexSpecification}.
	 */
	public static DropIndexSpecification dropIndex(@Nullable CqlIdentifier keyspace, CqlIdentifier indexName) {
		return DropIndexSpecification.dropIndex(keyspace, indexName);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with user type creation, alteration, and deletion.
	// -------------------------------------------------------------------------

	/**
	 * Entry point into the {@link CreateUserTypeSpecification}'s fluent API given {@code name} to create a type.
	 * Convenient if imported statically.
	 *
	 * @param typeName must not {@literal null} or empty.
	 * @return a new {@link CreateUserTypeSpecification}.
	 */
	public static CreateUserTypeSpecification createType(String typeName) {
		return CreateUserTypeSpecification.createType(typeName);
	}

	/**
	 * Entry point into the {@link CreateUserTypeSpecification}'s fluent API given {@code name} to create a type.
	 * Convenient if imported statically. Uses the default keyspace if {@code keyspace} is null; otherwise, of the
	 * {@code keyspace} is not {@literal null}, then the UDT name is prefixed with {@code keyspace}.
	 *
	 * @param keyspace can be {@literal null}.
	 * @param typeName must not {@literal null}.
	 * @return a new {@link CreateUserTypeSpecification}.
	 */
	public static CreateUserTypeSpecification createType(@Nullable String keyspace, String typeName) {
		return createType(getOptionalKeyspace(keyspace), CqlIdentifier.fromCql(typeName));
	}

	/**
	 * Entry point into the {@link CreateUserTypeSpecification}'s fluent API given {@code typeName} to create a type.
	 * Convenient if imported statically.
	 *
	 * @param name must not {@literal null}.
	 * @return a new {@link CreateUserTypeSpecification}.
	 */
	public static CreateUserTypeSpecification createType(CqlIdentifier name) {
		return CreateUserTypeSpecification.createType(name);
	}

	/**
	 * Entry point into the {@link CreateUserTypeSpecification}'s fluent API given {@code typeName} to create a type.
	 * Convenient if imported statically. Uses the default keyspace if {@code keyspace} is null; otherwise, of the
	 * {@code keyspace} is not {@literal null}, then the UDT name is prefixed with {@code keyspace}.
	 *
	 * @param keyspace can be {@literal null}.
	 * @param name must not {@literal null}.
	 * @return a new {@link CreateUserTypeSpecification}.
	 */
	public static CreateUserTypeSpecification createType(@Nullable CqlIdentifier keyspace, CqlIdentifier name) {
		return CreateUserTypeSpecification.createType(keyspace, name);
	}

	/**
	 * Entry point into the {@link AlterColumnSpecification}'s fluent API given {@code typeName} to alter a user type.
	 * Convenient if imported statically.
	 *
	 * @param typeName must not be {@literal null} or empty.
	 * @return a new {@link AlterUserTypeSpecification}.
	 */
	public static AlterUserTypeSpecification alterType(String typeName) {
		return AlterUserTypeSpecification.alterType(typeName);
	}

	/**
	 * Entry point into the {@link AlterUserTypeSpecification}'s fluent API given {@code typeName} to alter a type.
	 * Convenient if imported statically. Uses the default keyspace if {@code keyspace} is null; otherwise, of the
	 * {@code keyspace} is not {@literal null}, then the table name is prefixed with {@code keyspace}.
	 *
	 * @param keyspace can be {@literal null}.
	 * @param typeName must not be {@literal null}.
	 * @return a new {@link AlterUserTypeSpecification}.
	 */
	private static AlterUserTypeSpecification alterType(@Nullable String keyspace, String typeName) {
		return alterType(getOptionalKeyspace(keyspace), CqlIdentifier.fromCql(typeName));
	}

	/**
	 * Entry point into the {@link AlterUserTypeSpecification}'s fluent API given {@code typeName} to alter a type.
	 * Convenient if imported statically.
	 *
	 * @param typeName must not be {@literal null}.
	 * @return a new {@link AlterUserTypeSpecification}.
	 */
	private static AlterUserTypeSpecification alterType(CqlIdentifier typeName) {
		return AlterUserTypeSpecification.alterType(typeName);
	}

	/**
	 * Entry point into the {@link AlterUserTypeSpecification}'s fluent API given {@code typeName} to alter a type.
	 * Convenient if imported statically. Uses the default keyspace if {@code keyspace} is null; otherwise, of the
	 * {@code keyspace} is not {@literal null}, then the table name is prefixed with {@code keyspace}.
	 *
	 * @param keyspace can be {@literal null}.
	 * @param typeName must not be {@literal null}.
	 * @return a new {@link AlterUserTypeSpecification}.
	 */
	private static AlterUserTypeSpecification alterType(@Nullable CqlIdentifier keyspace, CqlIdentifier typeName) {
		return AlterUserTypeSpecification.alterType(keyspace, typeName);
	}

	/**
	 * Entry point into the {@link DropUserTypeSpecification}'s fluent API given {@code typeName} to drop a type.
	 * Convenient if imported statically.
	 *
	 * @param typeName must not be {@code null} or empty.
	 * @return a new {@link DropUserTypeSpecification}.
	 */
	public static DropUserTypeSpecification dropType(String typeName) {
		return DropUserTypeSpecification.dropType(typeName);
	}

	/**
	 * Entry point into the {@link DropUserTypeSpecification}'s fluent API given {@code typeName} to drop a type.
	 * Convenient if imported statically. Uses the default keyspace if {@code keyspace} is null; otherwise, of the
	 * {@code keyspace} is not {@literal null}, then the UDT name is prefixed with {@code keyspace}.
	 *
	 * @param keyspace can be {@code null}.
	 * @param typeName must not be {@code null} or empty.
	 * @return a new {@link DropUserTypeSpecification}.
	 */
	public static DropUserTypeSpecification dropType(@Nullable String keyspace, String typeName) {
		return dropType(getOptionalKeyspace(keyspace), CqlIdentifier.fromCql(typeName));
	}

	/**
	 * Entry point into the {@link DropUserTypeSpecification}'s fluent API given {@code typeName} to drop a type.
	 * Convenient if imported statically.
	 *
	 * @param typeName must not be {@code null} or empty.
	 * @return a new {@link DropUserTypeSpecification}.
	 */
	public static DropUserTypeSpecification dropType(CqlIdentifier typeName) {
		return DropUserTypeSpecification.dropType(typeName);
	}

	/**
	 * Entry point into the {@link DropUserTypeSpecification}'s fluent API given {@code typeName} to drop a type.
	 * Convenient if imported statically. Uses the default keyspace if {@code keyspace} is null; otherwise, of the
	 * {@code keyspace} is not {@literal null}, then the UDT name is prefixed with {@code keyspace}.
	 *
	 * @param keyspace can be {@code null}.
	 * @param typeName must not be {@code null} or empty.
	 * @return a new {@link DropUserTypeSpecification}.
	 */
	public static DropUserTypeSpecification dropType(@Nullable CqlIdentifier keyspace, CqlIdentifier typeName) {
		return DropUserTypeSpecification.dropType(keyspace, typeName);
	}

	@Nullable
	private static CqlIdentifier getOptionalKeyspace(@Nullable String keyspace) {
		return ObjectUtils.isEmpty(keyspace) ? null : CqlIdentifier.fromCql(keyspace);
	}

}
