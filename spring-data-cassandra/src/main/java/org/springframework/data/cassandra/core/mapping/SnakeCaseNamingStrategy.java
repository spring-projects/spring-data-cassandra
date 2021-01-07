/*
 * Copyright 2020-2021 the original author or authors.
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

import org.springframework.data.util.ParsingUtils;
import org.springframework.util.Assert;

/**
 * Naming strategy that renders {@literal CamelCase} name parts to {@literal snake_case}.
 *
 * @author Mark Paluch
 * @since 3.0
 */
public class SnakeCaseNamingStrategy implements NamingStrategy {

	public SnakeCaseNamingStrategy() {}

	/**
	 * Uses {@link Class#getSimpleName()} and separates camel case parts with '_'.
	 */
	public String getTableName(CassandraPersistentEntity<?> entity) {

		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		return ParsingUtils.reconcatenateCamelCase(entity.getType().getSimpleName(), "_");
	}

	/**
	 * Uses {@link Class#getSimpleName()} and separates camel case parts with '_'.
	 */
	public String getUserDefinedTypeName(CassandraPersistentEntity<?> entity) {

		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		return ParsingUtils.reconcatenateCamelCase(entity.getType().getSimpleName(), "_");
	}

	/**
	 * Uses {@link CassandraPersistentProperty#getName()} and separates camel case parts with '_'.
	 */
	public String getColumnName(CassandraPersistentProperty property) {

		Assert.notNull(property, "CassandraPersistentProperty must not be null");

		return ParsingUtils.reconcatenateCamelCase(property.getName(), "_");
	}
}
