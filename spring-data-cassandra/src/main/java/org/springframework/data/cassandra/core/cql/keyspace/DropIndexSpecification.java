/*
 * Copyright 2013-present the original author or authors.
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

import org.springframework.lang.Nullable;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Value object representing a {@code DROP INDEX} specification.
 *
 * @author Matthew T. Adams
 * @author David Webb
 * @author Mark Paluch
 */
public class DropIndexSpecification extends IndexNameSpecification<DropIndexSpecification> implements CqlSpecification {

	private DropIndexSpecification(@Nullable CqlIdentifier keyspace, CqlIdentifier name) {
		super(keyspace, name);
	}

	/**
	 * Create a new {@link DropIndexSpecification} for the given {@code indexName}.
	 *
	 * @param indexName must not be {@literal null} or empty.
	 * @return a new {@link DropIndexSpecification}.
	 */
	public static DropIndexSpecification dropIndex(String indexName) {
		return dropIndex(CqlIdentifier.fromCql(indexName));
	}

	/**
	 * Create a new {@link DropIndexSpecification} for the given {@code indexName}.
	 *
	 * @param indexName must not be {@literal null}.
	 * @return a new {@link DropIndexSpecification}.
	 */
	public static DropIndexSpecification dropIndex(CqlIdentifier indexName) {
		return new DropIndexSpecification(null, indexName);
	}

	/**
	 * Create a new {@link DropIndexSpecification} for the given {@code indexName}. Uses the default keyspace if
	 * {@code keyspace} is null; otherwise, of the {@code keyspace} is not {@link null}, then the index name is prefixed
	 * with {@code keyspace}.
	 *
	 * @param keyspace can be {@literal null}.
	 * @param indexName must not be {@literal null}.
	 * @return a new {@link DropIndexSpecification}.
	 */
	public static DropIndexSpecification dropIndex(@Nullable CqlIdentifier keyspace, CqlIdentifier indexName) {
		return new DropIndexSpecification(keyspace, indexName);
	}
}
