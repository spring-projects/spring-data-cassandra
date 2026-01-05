/*
 * Copyright 2024-present the original author or authors.
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

import org.springframework.data.cassandra.core.cql.keyspace.*;

/**
 * Entrypoint for CQL generation of {@link CqlSpecification} objects representing DML statements such as table creations
 * or keyspace drops. For example:
 *
 * <pre class="code">
 * DropUserTypeSpecification spec = SpecificationBuilder.dropType("address");
 * String cql = CqlGenerator.toCql(spec);
 * </pre>
 *
 * @author Mark Paluch
 * @since 4.4
 * @see SpecificationBuilder
 */
public final class CqlGenerator {

	private CqlGenerator() {
		// utility class, no instances
	}

	/**
	 * Entrypoint for CQL generation of {@link CqlSpecification} objects.
	 *
	 * @param specification the CQL specification to generate CQL for.
	 * @return the generated CQL from {@link CqlSpecification}.
	 */
	public static String toCql(CqlSpecification specification) {

		if (specification instanceof CreateKeyspaceSpecification createKeyspace) {
			return CreateKeyspaceCqlGenerator.toCql(createKeyspace);
		}

		if (specification instanceof AlterKeyspaceSpecification alterKeyspace) {
			return AlterKeyspaceCqlGenerator.toCql(alterKeyspace);
		}

		if (specification instanceof DropKeyspaceSpecification dropKeyspace) {
			return DropKeyspaceCqlGenerator.toCql(dropKeyspace);
		}

		if (specification instanceof CreateTableSpecification createTable) {
			return CreateTableCqlGenerator.toCql(createTable);
		}

		if (specification instanceof AlterTableSpecification alterTable) {
			return AlterTableCqlGenerator.toCql(alterTable);
		}

		if (specification instanceof DropTableSpecification dropTable) {
			return DropTableCqlGenerator.toCql(dropTable);
		}

		if (specification instanceof CreateUserTypeSpecification createType) {
			return CreateUserTypeCqlGenerator.toCql(createType);
		}

		if (specification instanceof AlterUserTypeSpecification alterType) {
			return AlterUserTypeCqlGenerator.toCql(alterType);
		}

		if (specification instanceof DropUserTypeSpecification dropType) {
			return DropUserTypeCqlGenerator.toCql(dropType);
		}

		if (specification instanceof CreateIndexSpecification createIndex) {
			return CreateIndexCqlGenerator.toCql(createIndex);
		}

		if (specification instanceof DropIndexSpecification dropIndex) {
			return DropIndexCqlGenerator.toCql(dropIndex);
		}

		throw new UnsupportedOperationException(String.format("CQL specification %s is not supported", specification));
	}

}
