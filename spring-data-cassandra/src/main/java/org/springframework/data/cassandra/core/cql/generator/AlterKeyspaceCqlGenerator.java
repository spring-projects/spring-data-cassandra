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

import org.springframework.data.cassandra.core.cql.keyspace.AlterKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.Option;

/**
 * CQL generator for generating {@code ALTER TABLE} statements.
 *
 * @author Matthew T. Adams
 */
public class AlterKeyspaceCqlGenerator extends KeyspaceOptionsCqlGenerator<AlterKeyspaceSpecification> {

	public AlterKeyspaceCqlGenerator(AlterKeyspaceSpecification specification) {
		super(specification);
	}

	public static String toCql(AlterKeyspaceSpecification specification) {
		return new AlterKeyspaceCqlGenerator(specification).toCql();
	}

	@Override
	public StringBuilder toCql(StringBuilder cql) {

		preambleCql(cql);
		optionsCql(cql);

		cql.append(";");

		return cql;
	}

	private void preambleCql(StringBuilder cql) {
		cql.append("ALTER KEYSPACE ").append(spec().getName().asCql(true)).append(" ");
	}

	@SuppressWarnings("unchecked")
	private void optionsCql(StringBuilder cql) {

		// begin options clause
		Map<String, Object> options = spec().getOptions();

		if (!options.isEmpty()) {

			// option preamble
			boolean first = true;
			cql.append(" WITH ");
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
