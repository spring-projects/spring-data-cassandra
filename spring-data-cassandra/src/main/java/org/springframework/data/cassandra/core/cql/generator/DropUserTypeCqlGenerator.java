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

import org.springframework.data.cassandra.core.cql.keyspace.DropUserTypeSpecification;

/**
 * CQL generator for generating a {@code DROP TYPE} statement.
 *
 * @author Fabio J. Mendes
 * @author Mark Paluch
 * @since 1.5
 * @see DropUserTypeSpecification
 */
public class DropUserTypeCqlGenerator extends UserTypeNameCqlGenerator<DropUserTypeSpecification> {

	/**
	 * Create a new {@link DropUserTypeCqlGenerator} for a given {@link DropUserTypeSpecification}.
	 *
	 * @param specification must not be {@literal null}.
	 */
	public DropUserTypeCqlGenerator(DropUserTypeSpecification specification) {
		super(specification);
	}

	public static String toCql(DropUserTypeSpecification specification) {
		return new DropUserTypeCqlGenerator(specification).toCql();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.generator.UserTypeNameCqlGenerator#toCql(java.lang.StringBuilder)
	 */
	@Override
	public StringBuilder toCql(StringBuilder cql) {
		return cql.append("DROP TYPE").append(spec().getIfExists() ? " IF EXISTS " : " ")
				.append(spec().getName().asCql(true))
				.append(";");
	}
}
