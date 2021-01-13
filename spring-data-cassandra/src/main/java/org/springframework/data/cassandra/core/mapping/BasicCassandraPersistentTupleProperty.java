/*
 * Copyright 2018-2021 the original author or authors.
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

import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Cassandra Tuple specific {@link CassandraPersistentProperty} implementation.
 *
 * @author Mark Paluch
 * @author Frank Spitulski
 * @since 2.1
 * @see Element
 */
public class BasicCassandraPersistentTupleProperty extends BasicCassandraPersistentProperty {

	private final @Nullable Integer ordinal;

	/**
	 * Create a new {@link BasicCassandraPersistentTupleProperty}.
	 *
	 * @param property the actual {@link Property} in the domain entity corresponding to this persistent entity.
	 * @param owner the containing object or {@link CassandraPersistentEntity} of this persistent property.
	 * @param simpleTypeHolder mapping of Java [simple|wrapper] types to Cassandra data types.
	 */
	public BasicCassandraPersistentTupleProperty(Property property, CassandraPersistentEntity<?> owner,
			SimpleTypeHolder simpleTypeHolder) {

		super(property, owner, simpleTypeHolder);

		this.ordinal = findOrdinal();
	}

	@Nullable
	private Integer findOrdinal() {

		if (isTransient()) {
			return null;
		}

		int ordinal;

		try {
			ordinal = getRequiredAnnotation(Element.class).value();
		} catch (IllegalStateException cause) {
			throw new MappingException(
					String.format("Missing @Element annotation in mapped tuple type for property [%s] in entity [%s]", getName(),
							getOwner().getName()),
					cause);
		}

		Assert.isTrue(ordinal >= 0,
				() -> String.format("Element ordinal must be greater or equal to zero for property [%s] in entity [%s]",
						getName(),
						getOwner().getName()));

		return ordinal;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty#getColumnName()
	 */
	@Override
	public CqlIdentifier getColumnName() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentProperty#getOrdinal()
	 */
	@Nullable
	@Override
	public Integer getOrdinal() {
		return this.ordinal;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty#isClusterKeyColumn()
	 */
	@Override
	public boolean isClusterKeyColumn() {
		return false;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty#isCompositePrimaryKey()
	 */
	@Override
	public boolean isCompositePrimaryKey() {
		return false;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty#isPartitionKeyColumn()
	 */
	@Override
	public boolean isPartitionKeyColumn() {
		return false;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty#isPrimaryKeyColumn()
	 */
	@Override
	public boolean isPrimaryKeyColumn() {
		return false;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty#setColumnName(org.springframework.data.cassandra.core.cql.CqlIdentifier)
	 */
	@Override
	public void setColumnName(CqlIdentifier columnName) {
		throw new UnsupportedOperationException("Cannot set a column name on a property representing a tuple element");
	}
}
