/*
 * Copyright 2013-2021 the original author or authors.
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

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Value object representing a {@code DROP INDEX} specification.
 *
 * @author Matthew T. Adams
 * @author David Webb
 * @author Mark Paluch
 */
public class DropIndexSpecification extends IndexNameSpecification<DropIndexSpecification> {

	private DropIndexSpecification(CqlIdentifier name) {
		super(name);
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
		return new DropIndexSpecification(indexName);
	}
}
