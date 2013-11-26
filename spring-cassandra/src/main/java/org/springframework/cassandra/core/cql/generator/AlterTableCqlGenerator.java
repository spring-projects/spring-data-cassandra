package org.springframework.cassandra.core.cql.generator;

import static org.springframework.cassandra.core.cql.CqlStringUtils.noNull;

import java.util.Map;

import org.springframework.cassandra.core.keyspace.AddColumnSpecification;
import org.springframework.cassandra.core.keyspace.AlterColumnSpecification;
import org.springframework.cassandra.core.keyspace.AlterTableSpecification;
import org.springframework.cassandra.core.keyspace.ColumnChangeSpecification;
import org.springframework.cassandra.core.keyspace.DropColumnSpecification;
import org.springframework.cassandra.core.keyspace.Option;

/**
 * CQL generator for generating <code>ALTER TABLE</code> statements.
 * 
 * @author Matthew T. Adams
 */
public class AlterTableCqlGenerator extends TableOptionsCqlGenerator<AlterTableSpecification> {

	public AlterTableCqlGenerator(AlterTableSpecification specification) {
		super(specification);
	}

	public StringBuilder toCql(StringBuilder cql) {
		cql = noNull(cql);

		preambleCql(cql);
		changesCql(cql);
		optionsCql(cql);

		cql.append(";");

		return cql;
	}

	protected StringBuilder preambleCql(StringBuilder cql) {
		return noNull(cql).append("ALTER TABLE ").append(spec().getNameAsIdentifier()).append(" ");
	}

	protected StringBuilder changesCql(StringBuilder cql) {
		cql = noNull(cql);

		boolean first = true;
		for (ColumnChangeSpecification change : spec().getChanges()) {
			if (first) {
				first = false;
			} else {
				cql.append(" ");
			}
			getCqlGeneratorFor(change).toCql(cql);
		}

		return cql;
	}

	protected ColumnChangeCqlGenerator<?> getCqlGeneratorFor(ColumnChangeSpecification change) {
		if (change instanceof AddColumnSpecification) {
			return new AddColumnCqlGenerator((AddColumnSpecification) change);
		}
		if (change instanceof DropColumnSpecification) {
			return new DropColumnCqlGenerator((DropColumnSpecification) change);
		}
		if (change instanceof AlterColumnSpecification) {
			return new AlterColumnCqlGenerator((AlterColumnSpecification) change);
		}
		throw new IllegalArgumentException("unknown ColumnChangeSpecification type: " + change.getClass().getName());
	}

	@SuppressWarnings("unchecked")
	protected StringBuilder optionsCql(StringBuilder cql) {
		cql = noNull(cql);

		Map<String, Object> options = spec().getOptions();
		if (options == null || options.isEmpty()) {
			return cql;
		}

		cql.append(" WITH ");
		boolean first = true;
		for (String key : options.keySet()) {
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
		return cql;
	}
}
