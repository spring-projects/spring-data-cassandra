/*
 * Copyright 2011-2013 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.core.cql.generator;

import static org.springframework.cassandra.core.cql.CqlStringUtils.noNull;
import static org.springframework.cassandra.core.PrimaryKeyType.PARTITIONED;
import static org.springframework.cassandra.core.PrimaryKeyType.CLUSTERED;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.cassandra.core.keyspace.ColumnSpecification;
import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.cassandra.core.keyspace.Option;

/**
 * CQL generator for generating a <code>CREATE TABLE</code> statement.
 * 
 * @author Matthew T. Adams
 * @author Alex Shvid
 */
public class CreateTableCqlGenerator extends TableCqlGenerator<CreateTableSpecification> {

	public static String toCql(CreateTableSpecification specification) {
		return new CreateTableCqlGenerator(specification).toCql();
	}

	public CreateTableCqlGenerator(CreateTableSpecification specification) {
		super(specification);
	}

	@Override
	public StringBuilder toCql(StringBuilder cql) {

		cql = noNull(cql);

		preambleCql(cql);
		columnsAndOptionsCql(cql);

		cql.append(";");

		return cql;
	}

	protected StringBuilder preambleCql(StringBuilder cql) {
		return noNull(cql).append("CREATE TABLE ").append(spec().getIfNotExists() ? "IF NOT EXISTS " : "")
				.append(spec().getNameAsIdentifier());
	}

	@SuppressWarnings("unchecked")
	protected StringBuilder columnsAndOptionsCql(StringBuilder cql) {

		cql = noNull(cql);

		// begin columns
		cql.append(" (");

		List<ColumnSpecification> partitionKeys = new ArrayList<ColumnSpecification>();
		List<ColumnSpecification> clusterKeys = new ArrayList<ColumnSpecification>();
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

		if (ordering != null || !options.isEmpty()) {

			// option preamble
			boolean first = true;
			cql.append(" WITH ");
			// end option preamble

			if (ordering != null) {
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

		return cql;
	}

	private static StringBuilder createOrderingClause(List<ColumnSpecification> columns) {
		StringBuilder ordering = null;
		boolean first = true;
		for (ColumnSpecification col : columns) {

			if (col.getOrdering() != null) { // then ordering specified
				if (ordering == null) { // then initialize ordering clause
					ordering = new StringBuilder().append("CLUSTERING ORDER BY (");
				}
				if (first) {
					first = false;
				} else {
					ordering.append(", ");
				}
				ordering.append(col.getName()).append(" ").append(col.getOrdering().cql());
			}
		}
		if (ordering != null) { // then end ordering option
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
			str.append(col.getName());

		}

	}

}
