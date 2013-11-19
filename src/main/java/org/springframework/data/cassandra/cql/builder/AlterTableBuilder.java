package org.springframework.data.cassandra.cql.builder;

import static org.springframework.data.cassandra.cql.CqlStringUtils.noNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.DataType;

public class AlterTableBuilder extends AbstractTableBuilder<AlterTableBuilder> {

	private List<ColumnChange> changes = new ArrayList<ColumnChange>();

	public AlterTableBuilder drop(String column) {
		changes.add(new DropColumn(column));
		return this;
	}

	public AlterTableBuilder add(String column, DataType type) {
		changes.add(new AddColumn(column, type));
		return this;
	}

	public AlterTableBuilder alter(String column, DataType type) {
		changes.add(new AlterColumn(column, type));
		return this;
	}

	public AlterTableBuilder name(String name) {
		return (AlterTableBuilder) super.name(name);
	}

	@Override
	public StringBuilder toCql(StringBuilder cql) {
		cql = noNull(cql);

		preambleCql(cql);
		changesCql(cql);
		optionsCql(cql);

		cql.append(";");

		return cql;
	}

	protected StringBuilder preambleCql(StringBuilder cql) {
		return noNull(cql).append("ALTER TABLE ").append(getNameAsIdentifier()).append(" ");
	}

	protected StringBuilder changesCql(StringBuilder cql) {
		cql = noNull(cql);

		boolean first = true;
		for (ColumnChange change : changes) {
			if (first) {
				first = false;
			} else {
				cql.append(" ");
			}
			change.toCql(cql);
		}

		return cql;
	}

	@SuppressWarnings("unchecked")
	protected StringBuilder optionsCql(StringBuilder cql) {
		cql = noNull(cql);

		Map<String, Object> options = options();
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
