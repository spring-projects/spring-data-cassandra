/*
 * Copyright 2016-2017 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.keyspace;

import static org.springframework.data.cassandra.core.cql.CqlIdentifier.*;
import static org.springframework.data.cassandra.core.cql.CqlStringUtils.*;

import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.util.Assert;

import com.datastax.driver.core.DataType;

/**
 * Builder class to specify fields.
 * <p/>
 * A {@link FieldSpecification} consists of a name and a {@link DataType}.
 *
 * @author Mark Paluch
 * @since 1.5
 * @see CqlIdentifier
 */
public class FieldSpecification {

	private CqlIdentifier name;
	private DataType type;

	/**
	 * Sets the field name.
	 *
	 * @param name must not be empty or {@literal null}.
	 * @return {@code this} {@link FieldSpecification}.
	 */
	public FieldSpecification name(String name) {
		return name(cqlId(name));
	}

	/**
	 * Sets the field name.
	 *
	 * @param name must not be {@literal null}.
	 * @return {@code this} {@link FieldSpecification}.
	 */
	public FieldSpecification name(CqlIdentifier name) {

		Assert.notNull(name, "CqlIdentifier must not be null");

		this.name = name;

		return this;
	}

	/**
	 * Sets the column's type.
	 *
	 * @param type The data type of the field, must not be {@literal null}.
	 * @return {@code this} {@link FieldSpecification}.
	 */
	public FieldSpecification type(DataType type) {

		Assert.notNull(type, "DataType must not be null");

		this.type = type;

		return this;
	}

	public String toCql() {
		return toCql(null).toString();
	}

	public StringBuilder toCql(StringBuilder cql) {
		return noNull(cql).append(name).append(" ").append(type);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return toCql(null).toString();
	}
}
