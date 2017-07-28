/*
 * Copyright 2013-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.cql.keyspace;

import static org.springframework.data.cassandra.core.cql.CqlIdentifier.*;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Builder class to construct a {@code CREATE INDEX} specification.
 *
 * @author Matthew T. Adams
 * @author David Webb
 * @author Mark Paluch
 */
public class CreateIndexSpecification extends IndexNameSpecification<CreateIndexSpecification>
		implements IndexDescriptor {

	private boolean ifNotExists = false;

	private boolean custom = false;

	private @Nullable CqlIdentifier tableName;

	private @Nullable CqlIdentifier columnName;

	private ColumnFunction columnFunction = ColumnFunction.NONE;

	private @Nullable String using;

	private Map<String, String> options = new LinkedHashMap<>();

	/**
	 * Entry point into the {@link CreateIndexSpecification}'s fluent API to create a index. Convenient if imported
	 * statically.
	 */
	public static CreateIndexSpecification createIndex() {
		return new CreateIndexSpecification();
	}

	/**
	 * Entry point into the {@link CreateIndexSpecification}'s fluent API to create a index. Convenient if imported
	 * statically.
	 */
	public static CreateIndexSpecification createIndex(CqlIdentifier name) {
		return new CreateIndexSpecification().name(name);
	}

	/**
	 * Entry point into the {@link CreateIndexSpecification}'s fluent API to create a index. Convenient if imported
	 * statically.
	 */
	public static CreateIndexSpecification createIndex(String name) {
		return new CreateIndexSpecification().name(name);
	}

	/**
	 * Causes the inclusion of an {@code IF NOT EXISTS} clause.
	 *
	 * @return this
	 */
	public CreateIndexSpecification ifNotExists() {
		return ifNotExists(true);
	}

	/**
	 * Toggles the inclusion of an {@code IF NOT EXISTS} clause.
	 *
	 * @return this
	 */
	public CreateIndexSpecification ifNotExists(boolean ifNotExists) {
		this.ifNotExists = ifNotExists;
		return this;
	}

	public boolean getIfNotExists() {
		return this.ifNotExists;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.keyspace.IndexDescriptor#isCustom()
	 */
	@Override
	public boolean isCustom() {
		return this.custom;
	}

	public CreateIndexSpecification using(String className) {

		if (StringUtils.hasText(className)) {
			this.using = className;
			this.custom = true;
		} else {
			this.using = null;
			this.custom = false;
		}

		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.keyspace.IndexDescriptor#getUsing()
	 */
	@Override
	public String getUsing() {
		return this.using;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.keyspace.IndexDescriptor#getColumnName()
	 */
	@Override
	public CqlIdentifier getColumnName() {
		return this.columnName;
	}

	/**
	 * Causes the inclusion of an {@code KEYS} clause.
	 *
	 * @return this
	 * @since 2.0
	 */
	public CreateIndexSpecification keys() {
		return columnFunction(ColumnFunction.KEYS);
	}

	/**
	 * Causes the inclusion of an {@code VALUES} clause.
	 *
	 * @return this
	 * @since 2.0
	 */
	public CreateIndexSpecification values() {
		return columnFunction(ColumnFunction.VALUES);
	}

	/**
	 * Causes the inclusion of an {@code ENTRIES} clause.
	 *
	 * @return this
	 * @since 2.0
	 */
	public CreateIndexSpecification entries() {
		return columnFunction(ColumnFunction.ENTRIES);
	}

	/**
	 * Causes the inclusion of an {@code FULL} clause.
	 *
	 * @return this
	 * @since 2.0
	 */
	public CreateIndexSpecification full() {
		return columnFunction(ColumnFunction.FULL);
	}

	/**
	 * Set a {@link ColumnFunction} such as {@code KEYS(…)}, {@code ENTRIES(…)}.
	 *
	 * @param columnFunction column function to apply, must not be {@literal null}.
	 * @return this
	 * @since 2.0
	 */
	public CreateIndexSpecification columnFunction(ColumnFunction columnFunction) {

		Assert.notNull(columnFunction, "ColumnFunction must not be null");

		this.columnFunction = columnFunction;

		return this;
	}

	public ColumnFunction getColumnFunction() {
		return this.columnFunction;
	}

	/**
	 * Configure a Index-creation options using key-value pairs.
	 *
	 * @param name option name.
	 * @param value option value.
	 * @return this
	 * @since 2.0
	 */
	public CreateIndexSpecification withOption(String name, String value) {

		this.options.put(name, value);

		return this;
	}

	/**
	 * @return index options map.
	 */
	public Map<String, String> getOptions() {
		return Collections.unmodifiableMap(this.options);
	}

	/**
	 * Sets the table name.
	 *
	 * @param tableName must not be {@literal null} or empty.
	 * @return this
	 */
	public CreateIndexSpecification tableName(String tableName) {
		return tableName(cqlId(tableName));
	}

	/**
	 * Sets the table name.
	 *
	 * @param tableName must not be {@literal null}.
	 * @return this
	 */
	public CreateIndexSpecification tableName(CqlIdentifier tableName) {

		Assert.notNull(tableName, "CqlIdentifier must not be null");

		this.tableName = tableName;

		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.keyspace.IndexDescriptor#getTableName()
	 */
	@Override
	public CqlIdentifier getTableName() {
		return this.tableName;
	}

	/**
	 * Sets the column name.
	 *
	 * @param columnName must not be {@literal null} or empty.
	 * @return this
	 */
	public CreateIndexSpecification columnName(String columnName) {
		return columnName(cqlId(columnName));
	}

	/**
	 * Sets the column name.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return this
	 */
	public CreateIndexSpecification columnName(CqlIdentifier columnName) {

		Assert.notNull(columnName, "CqlIdentifier must not be null");

		this.columnName = columnName;

		return this;
	}

	/**
	 * Column functions to specify indexing behavior.
	 *
	 * @since 2.0
	 */
	public enum ColumnFunction {

		/**
		 * Use the plain column value for indexing.
		 */
		NONE,

		/**
		 * Index keys for {@link Map} typed columns.
		 */
		KEYS,

		/**
		 * Index values for {@link Map} typed columns.
		 */
		VALUES,

		/**
		 * Index keys and values (entry-level indexing) for {@link Map} typed columns.
		 */
		ENTRIES,

		/**
		 * Index the entire {@link Collection}/{@link Map} as-is to match on whole collections/maps as predicate.
		 */
		FULL
	}
}
