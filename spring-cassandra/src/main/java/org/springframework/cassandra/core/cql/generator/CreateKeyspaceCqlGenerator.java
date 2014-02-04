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

import java.util.Map;

import org.springframework.cassandra.config.KeyspaceAttributes;
import org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.KeyspaceOption;
import org.springframework.cassandra.core.keyspace.Option;

/**
 * CQL generator for generating a <code>CREATE TABLE</code> statement.
 * 
 * @author Matthew T. Adams
 * @author Alex Shvid
 */
public class CreateKeyspaceCqlGenerator extends KeyspaceCqlGenerator<CreateKeyspaceSpecification> {

	public CreateKeyspaceCqlGenerator(CreateKeyspaceSpecification specification) {
		super(specification);
	}

	public StringBuilder toCql(StringBuilder cql) {

		cql = noNull(cql);

		preambleCql(cql);
		optionsCql(cql);

		cql.append(";");

		return cql;
	}

	protected StringBuilder preambleCql(StringBuilder cql) {
		return noNull(cql).append("CREATE KEYSPACE ").append(spec().getIfNotExists() ? "IF NOT EXISTS " : "")
				.append(spec().getNameAsIdentifier());
	}

	@SuppressWarnings("unchecked")
	protected StringBuilder optionsCql(StringBuilder cql) {
		cql = noNull(cql);

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

		return cql;
	}
}
