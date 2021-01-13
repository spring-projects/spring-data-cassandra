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

import org.springframework.data.cassandra.core.cql.keyspace.CreateUserTypeSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.FieldSpecification;
import org.springframework.util.Assert;

/**
 * CQL generator for generating a {@code CREATE TYPE} statement.
 *
 * @author Fabio J. Mendes
 * @author Mark Paluch
 * @author Frank Spitulski
 * @since 1.5
 * @see CreateUserTypeSpecification
 */
public class CreateUserTypeCqlGenerator extends UserTypeNameCqlGenerator<CreateUserTypeSpecification> {

	/**
	 * Create a new {@link CreateUserTypeCqlGenerator} for a given {@link CreateUserTypeSpecification}.
	 *
	 * @param specification must not be {@literal null}.
	 */
	public CreateUserTypeCqlGenerator(CreateUserTypeSpecification specification) {
		super(specification);
	}

	public static String toCql(CreateUserTypeSpecification specification) {
		return new CreateUserTypeCqlGenerator(specification).toCql();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.generator.UserTypeNameCqlGenerator#toCql(java.lang.StringBuilder)
	 */
	@Override
	public StringBuilder toCql(StringBuilder cql) {

		Assert.notNull(getSpecification().getName(), "User type name must not be null");

		Assert.isTrue(!getSpecification().getFields().isEmpty(),
				() -> String.format("User type [%s] does not contain fields", getSpecification().getName().asCql(true)));

		return columns(preambleCql(cql)).append(";");
	}

	private StringBuilder preambleCql(StringBuilder cql) {

		return cql.append("CREATE TYPE ").append(spec().getIfNotExists() ? "IF NOT EXISTS " : "")
				.append(spec().getName().asCql(true));
	}

	private StringBuilder columns(StringBuilder cql) {

		// begin columns
		cql.append(" (");

		boolean first = true;

		for (FieldSpecification column : spec().getFields()) {

			if (!first) {
				cql.append(", ");
			}

			column.toCql(cql);

			first = false;
		}

		cql.append(")");
		// end columns

		return cql;
	}
}
