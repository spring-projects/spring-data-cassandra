/*
 * Copyright 2013-2017 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.generator;

import static org.springframework.data.cassandra.core.cql.CqlStringUtils.*;

import org.springframework.data.cassandra.core.cql.keyspace.CreateIndexSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.CreateIndexSpecification.ColumnFunction;

/**
 * CQL generator for generating a {@code CREATE INDEX} statement.
 *
 * @author Matthew T. Adams
 * @author David Webb
 * @author Mark Paluch
 */
public class CreateIndexCqlGenerator extends IndexNameCqlGenerator<CreateIndexSpecification> {

	public static String toCql(CreateIndexSpecification specification) {
		return new CreateIndexCqlGenerator(specification).toCql();
	}

	public CreateIndexCqlGenerator(CreateIndexSpecification specification) {
		super(specification);
	}

	@Override
	public StringBuilder toCql(StringBuilder cql) {

		cql = noNull(cql);

		cql.append("CREATE").append(spec().isCustom() ? " CUSTOM" : "").append(" INDEX")
				.append(spec().getIfNotExists() ? " IF NOT EXISTS" : "");

		if (spec().getName() != null) {
			cql.append(" ").append(spec().getName());
		}

		cql.append(" ON ").append(spec().getTableName()).append(" (");

		if (spec().getColumnFunction() != ColumnFunction.NONE) {
			cql.append(spec().getColumnFunction().name()).append("(").append(spec().getColumnName()).append(")");
		} else {
			cql.append(spec().getColumnName());
		}

		cql.append(")");

		if (spec().isCustom()) {
			cql.append(" USING ").append("'").append(spec().getUsing()).append("'");
		}

		cql.append(";");

		return cql;
	}
}
