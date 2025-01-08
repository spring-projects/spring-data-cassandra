/*
 * Copyright 2016-2025 the original author or authors.
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

import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;

/**
 * Base value object class to specify user type fields.
 * <p>
 * A {@link FieldSpecification} consists of a name and a {@link DataType}.
 *
 * @author Mark Paluch
 * @since 1.5
 * @see CqlIdentifier
 */
public class FieldSpecification {

	private final CqlIdentifier name;
	private final DataType type;

	private FieldSpecification(CqlIdentifier name, DataType type) {

		Assert.notNull(name, "CqlIdentifier must not be null");
		Assert.notNull(type, "DataType must not be null");

		this.name = name;
		this.type = type;
	}

	/**
	 * Create a new {@link FieldSpecification} for the given {@code name} and {@link DataType}
	 *
	 * @param name must not be {@literal null} or empty.
	 * @param type must not be {@literal null}.
	 */
	public static FieldSpecification of(String name, DataType type) {
		return new FieldSpecification(CqlIdentifier.fromCql(name), type);
	}

	/**
	 * Create a new {@link FieldSpecification} given {@link CqlIdentifier name} and {@link DataType}.
	 *
	 * @param name must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @return
	 */
	public static FieldSpecification of(CqlIdentifier name, DataType type) {
		return new FieldSpecification(name, type);
	}

	public String toCql() {
		return toCql(new StringBuilder()).toString();
	}

	public StringBuilder toCql(StringBuilder cql) {
		return cql.append(name.asCql(true)).append(" ").append(type.asCql(true, true));
	}

	@Override
	public String toString() {
		return toCql(new StringBuilder()).toString();
	}
}
