package org.springframework.data.cassandra.cql;

import static org.springframework.data.cassandra.cql.CreateTable.Column.Key.PARTITION;
import static org.springframework.data.cassandra.cql.CreateTable.Column.Key.PRIMARY;
import static org.springframework.data.cassandra.cql.CreateTable.Column.Order.ASCENDING;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.cassandra.cql.CreateTable.Column.Key;
import org.springframework.data.cassandra.cql.CreateTable.Column.Order;
import org.springframework.util.Assert;

public class CreateTable {

	protected static StringBuilder ensure(StringBuilder sb) {
		return sb == null ? new StringBuilder() : sb;
	}

	public static class Column {

		public enum Key {
			PARTITION, PRIMARY
		}

		public enum Order {
			ASCENDING("ASC"), DESCENDING("DESC");

			private String cql;

			private Order(String cql) {
				this.cql = cql;
			}

			public String cql() {
				return cql;
			}
		}

		private String name;
		private String type;
		private Key key;
		private Order order = ASCENDING;

		public Column name(String name) {
			Assert.hasLength(name);
			this.name = name;
			return this;
		}

		public Column type(String type) {
			Assert.hasLength(type);
			this.type = type;
			return this;
		}

		public Column key(Key key) {
			return key(key, ASCENDING);
		}

		public Column key(Key key, Order order) {
			this.key = key;
			this.order = order;
			return this;
		}

		public void assertValid() {
			// TODO
		}

		public StringBuilder cql(StringBuilder cql) {
			return (cql = ensure(cql)).append(name).append(" ").append(type);
		}

		@Override
		public String toString() {
			return cql(null).toString();
		}
	}

	private boolean ifNotExists = false;
	private String name;
	private List<Column> columns = new ArrayList<Column>();
	private Map<String, Object> options = new HashMap<String, Object>();

	public CreateTable ifNotExists() {
		return ifNotExists(true);
	}

	public CreateTable ifNotExists(boolean ifNotExists) {
		this.ifNotExists = ifNotExists;
		return this;
	}

	public CreateTable name(String name) {
		Assert.hasLength(name);
		this.name = name;
		return this;
	}

	public CreateTable options(Map<String, Object> options) {
		this.options = options;
		return this;
	}

	public CreateTable option(String name, Object value) {
		options().put(name, value);
		return this;
	}

	public CreateTable columns(List<Column> columns) {
		columns().addAll(columns);
		return this;
	}

	public CreateTable column(String name, String type) {
		return column(name, type, null, null);
	}

	public CreateTable partition(String name, String type) {
		return partition(name, type, null);
	}

	public CreateTable partition(String name, String type, Order order) {
		return column(name, type, PARTITION, order);
	}

	public CreateTable primary(String name, String type) {
		return primary(name, type, null);
	}

	public CreateTable primary(String name, String type, Order order) {
		return column(name, type, PRIMARY, order);
	}

	public CreateTable column(String name, String type, Key key, Order order) {
		columns().add(new Column().name(name).type(type).key(key, order));
		return this;
	}

	protected List<Column> columns() {
		return columns == null ? columns = new ArrayList<Column>() : columns;
	}

	protected Map<String, Object> options() {
		return options == null ? options = new HashMap<String, Object>() : options;
	}

	public String cql() {
		return cql(true);
	}

	protected String cql(boolean validate) {
		if (validate) {
			assertValid();
		}

		StringBuilder cql = new StringBuilder();

		preamble(cql);
		columnsAndOptions(cql);

		cql.append(";");

		return cql.toString();
	}

	protected StringBuilder preamble(StringBuilder cql) {
		return (cql = ensure(cql)).append("CREATE TABLE ").append(ifNotExists ? "IF NOT EXISTS " : "").append(name);
	}

	@SuppressWarnings("unchecked")
	protected StringBuilder columnsAndOptions(StringBuilder cql) {

		cql = ensure(cql);

		// begin columns
		cql.append(" (");

		List<Column> partitionKeys = new ArrayList<Column>();
		List<Column> primaryKeys = new ArrayList<Column>();
		for (Column col : columns) {
			col.cql(cql).append(", ");

			if (col.key == PARTITION) {
				partitionKeys.add(col);
			} else if (col.key == PRIMARY) {
				primaryKeys.add(col);
			}
		}

		// begin primary key clause
		cql.append("PRIMARY KEY ");
		StringBuilder partitions = new StringBuilder();
		StringBuilder primaries = new StringBuilder();

		if (partitionKeys.size() > 1) {
			partitions.append("(");
		}

		StringBuilder clustering = null;

		boolean clusteringFirst = true;
		boolean first = true;
		for (Column col : partitionKeys) {
			if (first) {
				first = false;
			} else {
				partitions.append(", ");
			}
			partitions.append(col.name);

			if (col.order != null) { // then ordering specified
				if (clustering == null) { // then initialize clustering clause
					clustering = new StringBuilder().append("CLUSTERING ORDER BY (");
				}
				if (clusteringFirst) {
					clusteringFirst = false;
				} else {
					clustering.append(", ");
				}
				clustering.append(col.name).append(" ").append(col.order.cql());
			}
		}
		if (clustering != null) { // then end clustering option
			clustering.append(")");
		}
		if (partitionKeys.size() > 1) {
			partitions.append(")");
		}

		first = true;
		for (Column col : primaryKeys) {
			if (first) {
				first = false;
			} else {
				primaries.append(", ");
			}
			primaries.append(col.name);
		}
		boolean parenthesize = partitionKeys.size() + primaryKeys.size() > 1;

		cql.append(parenthesize ? "(" : "");
		cql.append(partitions);
		cql.append(primaryKeys.size() > 0 ? ", " : "");
		cql.append(primaries);
		cql.append(parenthesize ? ")" : "");
		// end primary key clause
		// end columns

		// begin options
		// begin option clause
		if (clustering != null || !options.isEmpty()) {
			// option preamble
			first = true;

			cql.append(" WITH ");

			if (clustering != null) {
				cql.append(clustering);
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
					if (value instanceof CharSequence) { // then value is a string
						cql.append(" = '").append(value.toString()).append("'");
						continue; // end string option
					}

					Map<String, Object> valueMap = null;
					if ((value instanceof Map) && !(valueMap = (Map<String, Object>) value).isEmpty()) {
						// then option value is a non-empty map

						// append { 'name' : 'value', ... }
						cql.append(" = { ");
						boolean mapFirst = true;
						for (Map.Entry<String, Object> entry : valueMap.entrySet()) {
							if (mapFirst) {
								mapFirst = false;
							} else {
								cql.append(", ");
							}

							cql.append("'").append(entry.getKey()).append("'"); // 'name'
							cql.append(" : ");
							Object entryValue = entry.getValue();
							cql.append("'").append(entryValue == null ? "" : entryValue.toString()).append("'"); // 'value'
						}
						cql.append(" } ");

						continue; // end non-empty value map
					}

					// else not a string, so just use unquoted string version of value
					cql.append(value.toString());
				}
			}
		}
		// end options

		return cql;
	}

	public void assertValid() {
		// TODO
	}

	@Override
	public String toString() {
		return cql(false);
	}
}
