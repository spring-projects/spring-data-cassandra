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
package org.springframework.data.cassandra.core.cql.generator;

import org.springframework.data.cassandra.core.cql.keyspace.ColumnChangeSpecification;
import org.springframework.util.Assert;

/**
 * Base class for column change CQL generators.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @param <T> The corresponding {@link ColumnChangeSpecification} type for this CQL generator.
 */
public abstract class ColumnChangeCqlGenerator<T extends ColumnChangeSpecification> {

	public abstract StringBuilder toCql(StringBuilder cql);

	private final ColumnChangeSpecification specification;

	public ColumnChangeCqlGenerator(ColumnChangeSpecification specification) {

		Assert.notNull(specification, "ColumnChangeSpecification must not be null");
		this.specification = specification;
	}

	@SuppressWarnings("unchecked")
	public T getSpecification() {
		return (T) specification;
	}

	protected T spec() {
		return getSpecification();
	}
}
