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

import org.springframework.data.cassandra.core.cql.keyspace.DropIndexSpecification;

/**
 * CQL generator for generating a {@code DROP INDEX} statement.
 *
 * @author Matthew T. Adams
 * @author David Webb
 */
public class DropIndexCqlGenerator extends IndexNameCqlGenerator<DropIndexSpecification> {

	public static String toCql(DropIndexSpecification specification) {
		return new DropIndexCqlGenerator(specification).toCql();
	}

	public DropIndexCqlGenerator(DropIndexSpecification specification) {
		super(specification);
	}

	@Override
	public StringBuilder toCql(StringBuilder cql) {
		return cql.append("DROP INDEX ")
				// .append(spec().getIfExists() ? "IF EXISTS " : "")
				.append(spec().getName().asCql(true)).append(";");
	}
}
