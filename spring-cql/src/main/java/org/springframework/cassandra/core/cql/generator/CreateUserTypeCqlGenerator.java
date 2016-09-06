/*
 * Copyright 2013-2014 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.core.cql.generator;

import static org.springframework.cassandra.core.cql.CqlStringUtils.noNull;

import org.springframework.cassandra.core.keyspace.ColumnSpecification;
import org.springframework.cassandra.core.keyspace.CreateUserTypeSpecification;

/**
 * CQL generator for generating a <code>CREATE TYPE</code> statement.
 * 
 * @author Fabio J. Mendes
 */
public class CreateUserTypeCqlGenerator extends UserTypeCqlGenerator<CreateUserTypeSpecification> {

	public static String toCql(CreateUserTypeSpecification specification) {
		return new CreateUserTypeCqlGenerator(specification).toCql();
	}

	public CreateUserTypeCqlGenerator(CreateUserTypeSpecification specification) {
		super(specification);
	}

	@Override
	public StringBuilder toCql(StringBuilder cql) {

		cql = noNull(cql);

		preambleCql(cql);
		columns(cql);

		cql.append(";");

		return cql;
	}

	protected StringBuilder preambleCql(StringBuilder cql) {
		return noNull(cql).append("CREATE TYPE ").append(spec().getIfNotExists() ? "IF NOT EXISTS " : "")
				.append(spec().getName());
	}

	protected StringBuilder columns(StringBuilder cql) {

		cql = noNull(cql);

		// begin columns
		cql.append(" (");
		for (ColumnSpecification col : spec().getColumns()) {
			col.toCql(cql).append(", ");
		}
		cql.delete(cql.length() - 2, cql.length());

		cql.append(")");
		// end columns

		return cql;
	}
}
