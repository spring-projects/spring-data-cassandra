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
package org.springframework.data.cassandra.core.cql.generator;

import org.jspecify.annotations.Nullable;

import org.springframework.data.cassandra.core.cql.keyspace.TableNameSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.UserTypeNameSpecification;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Utility to render {@link CqlIdentifier}.
 *
 * @author Mark Paluch
 * @since 4.4
 */
class CqlIdentifierUtil {

	/**
	 * Render a UDT name, potentially prefixed by a keyspace name.
	 */
	public static String renderName(UserTypeNameSpecification spec) {
		return renderName(spec.getKeyspace(), spec.getName());
	}

	/**
	 * Render a table name, potentially prefixed by a keyspace name.
	 */
	public static String renderName(TableNameSpecification spec) {
		return renderName(spec.getKeyspace(), spec.getName());
	}

	/**
	 * Render a {@code keyspace} and {@code name} tuple. Falls back to the {@code name} if {@code keyspace} is
	 * {@literal null} (absent).
	 */
	public static String renderName(@Nullable CqlIdentifier keyspace, CqlIdentifier name) {

		if (keyspace != null) {
			return String.format("%s.%s", keyspace.asCql(true), name.asCql(true));
		}

		return name.asCql(true);
	}
}
