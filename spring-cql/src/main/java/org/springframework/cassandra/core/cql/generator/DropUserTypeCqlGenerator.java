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

import org.springframework.cassandra.core.keyspace.DropUserTypeSpecification;

/**
 * CQL generator for generating a <code>DROP TYPE</code> statement.
 * 
 * @author Fabio J. Mendes
 */
public class DropUserTypeCqlGenerator extends UserTypeNameCqlGenerator<DropUserTypeSpecification> {

	public static String toCql(DropUserTypeSpecification specification) {
		return new DropUserTypeCqlGenerator(specification).toCql();
	}

	public DropUserTypeCqlGenerator(DropUserTypeSpecification specification) {
		super(specification);
	}

	@Override
	public StringBuilder toCql(StringBuilder cql) {
		return noNull(cql).append("DROP TYPE ")
		// .append(spec().getIfExists() ? "IF EXISTS " : "")
				.append(spec().getName()).append(";");
	}
}
