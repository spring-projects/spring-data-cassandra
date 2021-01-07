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

import org.springframework.data.cassandra.core.cql.keyspace.AddColumnSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.AlterColumnSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.AlterTableSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.ColumnChangeSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.DropColumnSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.Option;
import org.springframework.data.cassandra.core.cql.keyspace.RenameColumnSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption;

/**
 * CQL generator for generating {@code ALTER TABLE} statements.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @see AlterTableSpecification
 * @see TableOptionsCqlGenerator
 */
public class AlterTableCqlGenerator extends TableOptionsCqlGenerator<AlterTableSpecification> {

	/**
	 * Create a new {@link AlterTableCqlGenerator}.
	 *
	 * @param specification must not be {@literal null}.
	 */
	public AlterTableCqlGenerator(AlterTableSpecification specification) {
		super(specification);
	}

	/**
	 * Generates a CQL statement from the given {@code specification}.
	 *
	 * @param specification must not be {@literal null}.
	 * @return the generated CQL statement.
	 */
	public static String toCql(AlterTableSpecification specification) {
		return new AlterTableCqlGenerator(specification).toCql();
	}

	@Override
	public StringBuilder toCql(StringBuilder cql) {

		preambleCql(cql);

		if (!spec().getChanges().isEmpty()) {
			cql.append(' ');
			changesCql(cql);
		}

		if (!spec().getOptions().isEmpty()) {
			cql.append(' ');
			optionsCql(cql);
		}

		cql.append(";");

		return cql;
	}

	private void preambleCql(StringBuilder cql) {
		cql.append("ALTER TABLE ").append(spec().getName().asCql(true));
	}

	private void changesCql(StringBuilder cql) {

		boolean first = true;

		for (ColumnChangeSpecification change : spec().getChanges()) {
			if (first) {
				first = false;
			} else {
				cql.append(" ");
			}
			getCqlGeneratorFor(change).toCql(cql);
		}

	}

	private ColumnChangeCqlGenerator<?> getCqlGeneratorFor(ColumnChangeSpecification change) {

		if (change instanceof AddColumnSpecification) {
			return new AddColumnCqlGenerator((AddColumnSpecification) change);
		}

		if (change instanceof DropColumnSpecification) {
			return new DropColumnCqlGenerator((DropColumnSpecification) change);
		}

		if (change instanceof AlterColumnSpecification) {
			return new AlterColumnCqlGenerator((AlterColumnSpecification) change);
		}

		if (change instanceof RenameColumnSpecification) {
			return new RenameColumnCqlGenerator((RenameColumnSpecification) change);
		}

		throw new IllegalArgumentException("unknown ColumnChangeSpecification type: " + change.getClass().getName());
	}

	@SuppressWarnings("unchecked")
	private void optionsCql(StringBuilder cql) {

		Map<String, Object> options = spec().getOptions();

		if (options.isEmpty()) {
			return;
		}

		cql.append("WITH ");

		boolean first = true;

		for (String key : options.keySet()) {

			/*
			 * Compact storage is illegal on alter table.
			 */
			if (key.equals(TableOption.COMPACT_STORAGE.getName())) {
				throw new IllegalArgumentException("Alter table cannot contain the COMPACT STORAGE option");
			}

			if (first) {
				first = false;
			} else {
				cql.append(" AND ");
			}

			cql.append(key);

			Object value = options.get(key);

			if (value == null) {
				continue;
			}

			cql.append(" = ");

			if (value instanceof Map) {
				optionValueMap((Map<Option, Object>) value, cql);
				continue;
			}

			// else just use value as string
			cql.append(value.toString());
		}
	}
}
