/*
 * Copyright 2016-2021 the original author or authors.
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

import org.springframework.data.cassandra.core.cql.keyspace.TableNameSpecification;

/**
 * Useful test class that specifies just about as much as you can for a CQL generation test. Intended to be extended by
 * classes that contain methods annotated with {@link Test}. Everything is public because this is a test class with no
 * need for encapsulation, and it makes for easier reuse in other tests like integration tests (hint hint).
 *
 * @author Matthew T. Adams
 * @param <S> The type of the {@link TableNameSpecification}
 * @param <G> The type of the {@link TableNameCqlGenerator}
 */
abstract class AbstractTableOperationCqlGeneratorTest<S extends TableNameSpecification, G extends TableNameCqlGenerator<?>> {

	protected abstract S specification();

	protected abstract G generator();

	S specification;
	private G generator;
	String cql;

	void prepare() {
		this.specification = specification();
		this.generator = generator();
		this.cql = generateCql();
	}

	private String generateCql() {
		return generator.toCql();
	}
}
