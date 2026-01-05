/*
 * Copyright 2017-present the original author or authors.
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

import java.util.Locale;
import java.util.function.UnaryOperator;

import org.jspecify.annotations.Nullable;
import org.springframework.util.Assert;

/**
 * Interface and default implementation of a naming strategy. Defaults to table name based on {@link Class} and column
 * name based on property names. Names are used as-is without quoting. Lower-case, non-keyword names are used without
 * quoting. Upper-case, keyword or other names requiring quoting are used with quotes.
 * <p>
 * NOTE: Can also be used as an adapter. Create a lambda or an anonymous subclass and override any settings to implement
 * a different strategy on the fly.
 *
 * @author Mark Paluch
 * @since 3.0
 */
public interface NamingStrategy {

	/**
	 * Empty implementation of the interface utilizing only the default implementation.
	 * <p>
	 * Using this avoids creating essentially the same class over and over again.
	 *
	 * @since 3.3.6
	 */
	NamingStrategy CASE_SENSITIVE = new NamingStrategy() {};

	/**
	 * Default implementation converting all names to {@link String#toLowerCase()}.
	 */
	NamingStrategy INSTANCE = CASE_SENSITIVE.transform(s -> s.toLowerCase(Locale.ROOT));

	/**
	 * Naming strategy that renders CamelCase name parts to {@code snake_case}.
	 */
	NamingStrategy SNAKE_CASE = new SnakeCaseNamingStrategy();

	/**
	 * Create a keyspace name from the given {@link CassandraPersistentEntity}. Defaults to {@literal null} meaning the
	 * keyspace is being inherited from the Cassandra session instead of using a specific keyspace regardless of the
	 * session configuration.
	 *
	 * @since 4.4
	 */
	@Nullable
	default String getKeyspace(CassandraPersistentEntity<?> entity) {
		return null;
	}

	/**
	 * Create a table name from the given {@link CassandraPersistentEntity}.
	 */
	default String getTableName(CassandraPersistentEntity<?> entity) {

		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		return entity.getType().getSimpleName();
	}

	/**
	 * Create a user-defined type name from the given {@link CassandraPersistentEntity}.
	 */
	default String getUserDefinedTypeName(CassandraPersistentEntity<?> entity) {

		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		return entity.getType().getSimpleName();
	}

	/**
	 * Create a column name from the given {@link CassandraPersistentProperty property}.
	 */
	default String getColumnName(CassandraPersistentProperty property) {

		Assert.notNull(property, "CassandraPersistentProperty must not be null");

		return property.getName();
	}

	/**
	 * Apply a {@link UnaryOperator transformation function} to create a new {@link NamingStrategy} that applies the given
	 * transformation to each name component. Example:
	 * <p class="code">
	 * NamingStrategy lower = NamingStrategy.INSTANCE.transform(String::toLowerCase);
	 * </p>
	 *
	 * @param mappingFunction must not be {@literal null}.
	 * @return the {@link NamingStrategy} that applies the given {@link UnaryOperator transformation function}.
	 */
	default NamingStrategy transform(UnaryOperator<String> mappingFunction) {

		Assert.notNull(mappingFunction, "Mapping function must not be null");

		NamingStrategy previous = this;

		return new TransformingNamingStrategy(previous, mappingFunction);
	}
}
