/*
 * Copyright 2013-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.cql.keyspace;

import static org.springframework.data.cassandra.core.cql.Ordering.*;
import static org.springframework.data.cassandra.core.cql.PrimaryKeyType.*;

import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;

/**
 * Object to configure a CQL column specification.
 * <p/>
 * Use {@link #name(String)} and {@link #type(String)} to set the name and type of the column, respectively. To specify
 * a clustered {@code PRIMARY KEY} column, use {@link #clustered()} or {@link #clustered(Ordering)}. To specify that the
 * {@code PRIMARY KEY} column is or is part of the partition key, use {@link #partitioned()} instead of
 * {@link #clustered()} or {@link #clustered(Ordering)}.
 *
 * @author Matthew T. Adams
 * @author Alex Shvid
 * @author Mark Paluch
 */
public class ColumnSpecification {

	/**
	 * Default ordering of primary key fields; value is {@link Ordering#ASCENDING}.
	 */
	public static final Ordering DEFAULT_ORDERING = ASCENDING;

	private final CqlIdentifier name;

	private @Nullable DataType type;

	private @Nullable PrimaryKeyType keyType;

	private @Nullable Ordering ordering;

	private ColumnSpecification(CqlIdentifier name) {
		this.name = name;
	}

	/**
	 * Create a new {@link ColumnSpecification} for the given {@code name}.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @return a new {@link ColumnSpecification} for {@code name}.
	 */
	public static ColumnSpecification name(String name) {
		return name(CqlIdentifier.fromCql(name));
	}

	/**
	 * Create a new {@link ColumnSpecification} for the given {@code name}.
	 *
	 * @param name must not be {@literal null}.
	 * @return a new {@link ColumnSpecification} for {@code name}.
	 */
	public static ColumnSpecification name(CqlIdentifier name) {

		Assert.notNull(name, "CqlIdentifier must not be null");

		return new ColumnSpecification(name);
	}

	/**
	 * Sets the column's type.
	 *
	 * @return this
	 */
	public ColumnSpecification type(DataType type) {

		Assert.notNull(type, "DataType must not be null!");

		this.type = type;

		return this;
	}

	/**
	 * Identifies this column as a primary key column that is also part of a partition key. Sets the column's
	 * {@link #keyType} to {@link PrimaryKeyType#PARTITIONED} and its {@link #ordering} to {@literal null}.
	 *
	 * @return this
	 */
	public ColumnSpecification partitioned() {
		return partitioned(true);
	}

	/**
	 * Toggles the identification of this column as a primary key column that also is or is part of a partition key. Sets
	 * {@link #ordering} to {@literal null} and, if the given boolean is <code>true</code>, then sets the column's
	 * {@link #keyType} to {@link PrimaryKeyType#PARTITIONED}, else sets it to {@literal null}.
	 *
	 * @return this
	 */
	public ColumnSpecification partitioned(boolean partitioned) {

		this.keyType = partitioned ? PARTITIONED : null;
		this.ordering = null;

		return this;
	}

	/**
	 * Identifies this column as a clustered key column with default ordering. Sets the column's {@link #keyType} to
	 * {@link PrimaryKeyType#CLUSTERED} and its {@link #ordering} to {@link #DEFAULT_ORDERING}.
	 *
	 * @return this
	 */
	public ColumnSpecification clustered() {
		return clustered(DEFAULT_ORDERING);
	}

	/**
	 * Identifies this column as a clustered key column with the given ordering. Sets the column's {@link #keyType} to
	 * {@link PrimaryKeyType#CLUSTERED} and its {@link #ordering} to the given {@link Ordering}.
	 *
	 * @return this
	 */
	public ColumnSpecification clustered(Ordering order) {
		return clustered(order, true);
	}

	/**
	 * Toggles the identification of this column as a clustered key column. If the given boolean is {@code true}, then
	 * sets the column's {@link #keyType} to {@link PrimaryKeyType#PARTITIONED} and {@link #ordering} to the given
	 * {@link Ordering} , else sets both {@link #keyType} and {@link #ordering} to {@literal null}.
	 *
	 * @return this
	 */
	public ColumnSpecification clustered(Ordering order, boolean primary) {

		this.keyType = primary ? CLUSTERED : null;
		this.ordering = primary ? order : null;

		return this;
	}

	/**
	 * Sets the column's {@link #keyType}.
	 *
	 * @return this
	 */
	public ColumnSpecification keyType(PrimaryKeyType keyType) {

		this.keyType = keyType;

		return this;
	}

	/**
	 * Sets the column's {@link #ordering}.
	 *
	 * @return this
	 */
	public ColumnSpecification ordering(Ordering ordering) {

		this.ordering = ordering;

		return this;
	}

	public CqlIdentifier getName() {
		return name;
	}

	@Nullable
	public DataType getType() {
		return type;
	}

	@Nullable
	public PrimaryKeyType getKeyType() {
		return keyType;
	}

	@Nullable
	public Ordering getOrdering() {
		return ordering;
	}

	public String toCql() {
		return toCql(new StringBuilder()).toString();
	}

	public StringBuilder toCql(StringBuilder cql) {
		return cql.append(name.asCql(true)).append(" ").append(type.asCql(true, true));
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return toCql(new StringBuilder()).append(" /* keyType=").append(keyType).append(", ordering=").append(ordering)
				.append(" */ ").toString();
	}
}
