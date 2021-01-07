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

import org.springframework.data.cassandra.core.cql.keyspace.ColumnChangeSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.RenameColumnSpecification;

/**
 * CQL generator for generating an {@code RENAME} column clause of an {@code ALTER TABLE} statement.
 *
 * @author Mark Paluch
 * @since 1.5
 * @see ColumnChangeCqlGenerator
 * @see RenameColumnSpecification
 * @see org.springframework.data.cassandra.core.cql.keyspace.AlterTableSpecification
 */
public class RenameColumnCqlGenerator extends ColumnChangeCqlGenerator<RenameColumnSpecification> {

	static final String RENAME = "RENAME";

	private final String keyword;

	RenameColumnCqlGenerator(RenameColumnSpecification specification) {
		this(RENAME, specification);
	}

	/**
	 * @param keyword the keyword to use for {@code RENAME}.
	 * @param specification the specification.
	 */
	RenameColumnCqlGenerator(String keyword, ColumnChangeSpecification specification) {
		super(specification);
		this.keyword = keyword;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.generator.ColumnChangeCqlGenerator#toCql(java.lang.StringBuilder)
	 */
	public StringBuilder toCql(StringBuilder cql) {
		return cql.append(keyword).append(' ').append(spec().getName().asCql(true)).append(" TO ")
				.append(spec().getTargetName().asCql(true));
	}
}
