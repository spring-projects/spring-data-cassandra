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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.data.cassandra.core.cql.keyspace.ColumnSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.CreateTableSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.Option;
import org.springframework.data.cassandra.core.cql.keyspace.TableSpecification;
import org.springframework.util.StringUtils;

import static org.springframework.data.cassandra.core.cql.PrimaryKeyType.*;

/**
 * CQL generator for generating a {@code CREATE TABLE} statement.
 *
 * @author Matthew T. Adams
 * @author Alex Shvid
 * @author Mark Paluch
 */
public class CreateTableCqlGenerator extends TableOptionsCqlGenerator<TableSpecification<CreateTableSpecification>> {

	public CreateTableCqlGenerator(CreateTableSpecification specification) {
		super(specification);
	}

	public static String toCql(CreateTableSpecification specification) {
		return new CreateTableCqlGenerator(specification).toCql();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.generator.TableOptionsCqlGenerator#spec()
	 */
	@Override
	protected CreateTableSpecification spec() {
		return (CreateTableSpecification) super.spec();
	}

	@Override
	public StringBuilder toCql(StringBuilder cql) {

		preambleCql(cql);
		columnsAndOptionsCql(cql);

		cql.append(";");

		return cql;
	}

	private void preambleCql(StringBuilder cql) {
		cql.append("CREATE TABLE ").append(spec().getIfNotExists() ? "IF NOT EXISTS " : "")
				.append(spec().getName().asCql(true));
	}

	@SuppressWarnings("unchecked")
	private void columnsAndOptionsCql(StringBuilder cql) {

		// begin columns
		cql.append(" (");

		List<ColumnSpecification> partitionKeys = new ArrayList<>();
		List<ColumnSpecification> clusterKeys = new ArrayList<>();
		for (ColumnSpecification col : spec().getColumns()) {
			col.toCql(cql).append(", ");

			if (col.getKeyType() == PARTITIONED) {
				partitionKeys.add(col);
			} else if (col.getKeyType() == CLUSTERED) {
				clusterKeys.add(col);
			}
		}

		// begin primary key clause
		cql.append("PRIMARY KEY (");

		if (partitionKeys.size() > 1) {
			// begin partition key clause
			cql.append("(");
		}

		appendColumnNames(cql, partitionKeys);

		if (partitionKeys.size() > 1) {
			cql.append(")");
			// end partition key clause
		}

		if (!clusterKeys.isEmpty()) {
			cql.append(", ");
		}

		appendColumnNames(cql, clusterKeys);

		cql.append(")");
		// end primary key clause

		cql.append(")");
		// end columns

		StringBuilder ordering = createOrderingClause(clusterKeys);
		// begin options
		// begin option clause
		Map<String, Object> options = spec().getOptions();

		if (!options.isEmpty() || StringUtils.hasText(ordering)) {

			// option preamble
			boolean first = true;
			cql.append(" WITH ");
			// end option preamble

			if (StringUtils.hasText(ordering)) {
				cql.append(ordering);
				first = false;
			}

			if (!options.isEmpty()) {
				for (String name : options.keySet()) {
					// append AND if we're not on first option
					if (first) {
						first = false;
					} else {
						cql.append(" AND ");
					}

					// append <name> = <value>
					cql.append(name);

					Object value = options.get(name);
					if (value == null) { // then assume string-only, valueless option like "COMPACT STORAGE"
						continue;
					}

					cql.append(" = ");

					if (value instanceof Map) {
						optionValueMap((Map<Option, Object>) value, cql);
						continue; // end non-empty value map
					}

					// else just use value as string
					cql.append(value.toString());
				}
			}
		}
		// end options
	}

	private static StringBuilder createOrderingClause(List<ColumnSpecification> columns) {

		StringBuilder ordering = new StringBuilder();
		boolean first = true;
		for (ColumnSpecification col : columns) {

			if (col.getOrdering() != null) { // then ordering specified
				if (!StringUtils.hasText(ordering)) { // then initialize ordering clause
					ordering.append("CLUSTERING ORDER BY (");
				}
				if (first) {
					first = false;
				} else {
					ordering.append(", ");
				}
				ordering.append(col.getName().asCql(true)).append(" ").append(col.getOrdering().cql());
			}
		}

		if (StringUtils.hasText(ordering)) { // then end ordering option
			ordering.append(")");
		}

		return ordering;
	}

	private static void appendColumnNames(StringBuilder str, List<ColumnSpecification> columns) {

		boolean first = true;
		for (ColumnSpecification col : columns) {
			if (first) {
				first = false;
			} else {
				str.append(", ");
			}
			str.append(col.getName().asCql(true));
		}
	}
}
