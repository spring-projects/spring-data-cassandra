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

import java.util.Map;

import org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceAttributes;
import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceOption;
import org.springframework.data.cassandra.core.cql.keyspace.Option;

/**
 * CQL generator for generating a {@code CREATE TABLE} statement.
 *
 * @author Matthew T. Adams
 * @author Alex Shvid
 */
public class CreateKeyspaceCqlGenerator extends KeyspaceOptionsCqlGenerator<CreateKeyspaceSpecification> {

	public CreateKeyspaceCqlGenerator(CreateKeyspaceSpecification specification) {
		super(specification);
	}

	public static String toCql(CreateKeyspaceSpecification specification) {
		return new CreateKeyspaceCqlGenerator(specification).toCql();
	}

	@Override
	public StringBuilder toCql(StringBuilder cql) {

		preambleCql(cql);
		optionsCql(cql);

		cql.append(";");

		return cql;
	}

	private void preambleCql(StringBuilder cql) {
		cql.append("CREATE KEYSPACE ").append(spec().getIfNotExists() ? "IF NOT EXISTS " : "")
				.append(spec().getName().asCql(true));
	}

	@SuppressWarnings("unchecked")
	private void optionsCql(StringBuilder cql) {

		cql.append(" ");

		// begin options clause
		Map<String, Object> options = spec().getOptions();

		Object replicationOption = options.get(KeyspaceOption.REPLICATION.getName());
		if (replicationOption == null) {
			Map<Option, Object> simpleReplicationMap = KeyspaceAttributes.newSimpleReplication();
			spec().with(KeyspaceOption.REPLICATION, simpleReplicationMap);
		}

		Object durableWrites = options.get(KeyspaceOption.DURABLE_WRITES.getName());
		if (durableWrites == null) {
			spec().with(KeyspaceOption.DURABLE_WRITES, Boolean.TRUE);
		}

		if (!options.isEmpty()) {

			// option preamble
			boolean first = true;
			cql.append("WITH ");
			// end option preamble

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
}
