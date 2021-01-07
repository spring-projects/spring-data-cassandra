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

import org.springframework.data.cassandra.core.cql.keyspace.DropTableSpecification;

/**
 * CQL generator for generating a {@code DROP TABLE} statement.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @see org.springframework.data.cassandra.core.cql.generator.TableNameCqlGenerator
 * @see org.springframework.data.cassandra.core.cql.keyspace.DropTableSpecification
 */
public class DropTableCqlGenerator extends TableNameCqlGenerator<DropTableSpecification> {

	public static String toCql(DropTableSpecification specification) {
		return new DropTableCqlGenerator(specification).toCql();
	}

	public DropTableCqlGenerator(DropTableSpecification specification) {
		super(specification);
	}

	@Override
	public StringBuilder toCql(StringBuilder cql) {

		DropTableSpecification specification = spec();

		return cql.append("DROP TABLE ").append(specification.getIfExists() ? "IF EXISTS " : "")
				.append(specification.getName().asCql(true)).append(";");
	}
}
