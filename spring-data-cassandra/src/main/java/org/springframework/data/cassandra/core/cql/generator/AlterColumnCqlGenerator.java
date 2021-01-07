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

import org.springframework.data.cassandra.core.cql.keyspace.AlterColumnSpecification;

/**
 * CQL generator for generating an {@code ALTER} column clause of an <code>ALTER TABLE</code> statement.
 *
 * @author Matthew T. Adams
 * @see AlterColumnSpecification
 * @see ColumnChangeCqlGenerator
 */
public class AlterColumnCqlGenerator extends ColumnChangeCqlGenerator<AlterColumnSpecification> {

	public AlterColumnCqlGenerator(AlterColumnSpecification specification) {
		super(specification);
	}

	public StringBuilder toCql(StringBuilder cql) {
		return cql.append("ALTER ").append(spec().getName().asCql(true)).append(" TYPE ")
				.append(spec().getType().asCql(true, true));
	}
}
