/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.cassandra.core.keyspace;

import static org.springframework.cassandra.core.Ordering.ASCENDING;
import static org.springframework.cassandra.core.PrimaryKeyType.CLUSTERED;
import static org.springframework.cassandra.core.PrimaryKeyType.PARTITIONED;
import static org.springframework.cassandra.core.cql.CqlStringUtils.noNull;

import org.springframework.cassandra.core.CqlIdentifier;
import org.springframework.cassandra.core.Ordering;
import org.springframework.cassandra.core.PrimaryKeyType;

import com.datastax.driver.core.DataType;

/**
 * Builder class to specify columns.
 * <p/>
 * Use {@link #name(String)} and {@link #type(String)} to set the name and type of the column, respectively. To specify
 * a clustered <code>PRIMARY KEY</code> column, use {@link #clustered()} or {@link #clustered(Ordering)}. To specify
 * that the <code>PRIMARY KEY</code> column is or is part of the partition key, use {@link #partitioned()} instead of
 * {@link #clustered()} or {@link #clustered(Ordering)}.
 * 
 * @author Matthew T. Adams
 * @author Alex Shvid
 */
public class ColumnSpecification {

	/**
	 * Default ordering of primary key fields; value is {@link Ordering#ASCENDING}.
	 */
	public static final Ordering DEFAULT_ORDERING = ASCENDING;

	private CqlIdentifier identifier;
	private DataType type; // TODO: determining if we should be coupling this to Datastax Java Driver type?
	private PrimaryKeyType keyType;
	private Ordering ordering;

	/**
	 * Sets the column's name.
	 * 
	 * @return this
	 */
	public ColumnSpecification name(String name) {
		identifier = new CqlIdentifier(name);
		return this;
	}

	/**
	 * Sets the column's type.
	 * 
	 * @return this
	 */
	public ColumnSpecification type(DataType type) {
		this.type = type;
		return this;
	}

	/**
	 * Identifies this column as a primary key column that is also part of a partition key. Sets the column's
	 * {@link #keyType} to {@link PrimaryKeyType#PARTITIONED} and its {@link #ordering} to <code>null</code>.
	 * 
	 * @return this
	 */
	public ColumnSpecification partitioned() {
		return partitioned(true);
	}

	/**
	 * Toggles the identification of this column as a primary key column that also is or is part of a partition key. Sets
	 * {@link #ordering} to <code>null</code> and, if the given boolean is <code>true</code>, then sets the column's
	 * {@link #keyType} to {@link PrimaryKeyType#PARTITIONED}, else sets it to <code>null</code>.
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
	 * Toggles the identification of this column as a clustered key column. If the given boolean is <code>true</code>,
	 * then sets the column's {@link #keyType} to {@link PrimaryKeyType#PARTITIONED} and {@link #ordering} to the given
	 * {@link Ordering} , else sets both {@link #keyType} and {@link #ordering} to <code>null</code>.
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
	/* package */ColumnSpecification keyType(PrimaryKeyType keyType) {
		this.keyType = keyType;
		return this;
	}

	/**
	 * Sets the column's {@link #ordering}.
	 * 
	 * @return this
	 */
	/* package */ColumnSpecification ordering(Ordering ordering) {
		this.ordering = ordering;
		return this;
	}

	public String getName() {
		return identifier.getName();
	}

	public String getNameAsIdentifier() {
		return identifier.toCql();
	}

	public DataType getType() {
		return type;
	}

	public PrimaryKeyType getKeyType() {
		return keyType;
	}

	public Ordering getOrdering() {
		return ordering;
	}

	public String toCql() {
		return toCql(null).toString();
	}

	public StringBuilder toCql(StringBuilder cql) {
		return (cql = noNull(cql)).append(identifier).append(" ").append(type);
	}

	@Override
	public String toString() {
		return toCql(null).append(" /* keyType=").append(keyType).append(", ordering=").append(ordering).append(" */ ")
				.toString();
	}
}