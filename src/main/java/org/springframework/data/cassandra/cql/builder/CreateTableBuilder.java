package org.springframework.data.cassandra.cql.builder;

import static org.springframework.data.cassandra.cql.CqlStringUtils.checkQuotedIdentifier;
import static org.springframework.data.cassandra.cql.CqlStringUtils.ensureNotNull;
import static org.springframework.data.cassandra.cql.CqlStringUtils.escapeSingle;
import static org.springframework.data.cassandra.cql.CqlStringUtils.singleQuote;
import static org.springframework.data.cassandra.mapping.KeyType.PARTITION;
import static org.springframework.data.cassandra.mapping.KeyType.PRIMARY;
import static org.springframework.data.cassandra.mapping.Ordering.ASCENDING;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.cassandra.cql.CqlStringUtils;
import org.springframework.data.cassandra.mapping.KeyType;
import org.springframework.data.cassandra.mapping.Ordering;

/**
 * Builder class to construct CQL for a <code>CREATE TABLE</code> statement. Not threadsafe.
 * 
 * @author Matthew T. Adams
 */
public class CreateTableBuilder {

	private boolean ifNotExists = false;
	private String name;
	private List<ColumnBuilder> columns = new ArrayList<ColumnBuilder>();
	private Map<String, Object> options = new HashMap<String, Object>();

	/**
	 * Causes the inclusion of an <code>IF NOT EXISTS</code> clause.
	 * 
	 * @return this
	 */
	public CreateTableBuilder ifNotExists() {
		return ifNotExists(true);
	}

	/**
	 * Toggles the inclusion of an <code>IF NOT EXISTS</code> clause.
	 * 
	 * @return this
	 */
	public CreateTableBuilder ifNotExists(boolean ifNotExists) {
		this.ifNotExists = ifNotExists;
		return this;
	}

	/**
	 * Sets the table name. Quotes are not escaped.
	 * 
	 * @see CqlStringUtils#escape(CharSequence)
	 * @see CqlStringUtils#scrub(CharSequence)
	 * 
	 * @return this
	 */
	public CreateTableBuilder name(String name) {
		checkQuotedIdentifier(name);
		this.name = name;
		return this;
	}

	/**
	 * Adds the given single-string option with no value to this table's options. Convenient overload of
	 * <code>with(string, null, false, false)</code>.
	 * 
	 * @param singleStringOption
	 * @return
	 */
	public CreateTableBuilder with(String string) {
		return with(string, null, false, false);
	}

	/**
	 * Adds the given single-quote-escaped then single-quoted option value by name to this table's options. Convenient
	 * overload of <code>with(name, value, true, true)</code>
	 * 
	 * @see #with(String, Object, boolean, boolean)
	 * @return this
	 */
	public CreateTableBuilder withQuoted(String name, Object value) {
		return with(name, value, true, true);
	}

	/**
	 * Adds the given option value by name with no quoting or escaping to this table's options. Convenient overload of
	 * <code>with(name, value, false, false)</code>
	 * 
	 * @see #with(String, Object, boolean, boolean)
	 * @return this
	 */
	public CreateTableBuilder withUnquoted(String name, Object value) {
		return with(name, value, false, false);
	}

	public CreateTableBuilder with(String name, Map<String, Object> valueMap) {
		return with(name, valueMap, false, false);
	}

	/**
	 * Adds the given option by name to this table's options.
	 * <p/>
	 * Options that have <code>null</code> values are considered single string options where the name of the option is the
	 * string to be used. Otherwise, the result of {@link Object#toString()} is considered to be the value of the option
	 * with the given name. The value, after conversion to string, may have embedded single quotes escaped according to
	 * parameter <code>escape</code> and may be single-quoted according to parameter <code>quote</code>.
	 * 
	 * @param name The name of the option
	 * @param value The value of the option. If <code>null</code>, the value is ignored and the option is considered to be
	 *          composed of only the name, otherwise the value's {@link Object#toString()} value is used.
	 * @param escape Whether to escape the value via {@link CqlStringUtils#escapeSingle(Object)}. Ignored if given value
	 *          is an instance of a {@link Map}.
	 * @param quote Whether to quote the value via {@link CqlStringUtils#singleQuote(Object)}. Ignored if given value is
	 *          an instance of a {@link Map}.
	 * @return this
	 */
	public CreateTableBuilder with(String name, Object value, boolean escape, boolean quote) {
		if (!(value instanceof Map)) {
			if (escape) {
				value = escapeSingle(value);
			}
			if (quote) {
				value = singleQuote(value);
			}
		}
		options().put(name, value);
		return this;
	}

	public CreateTableBuilder column(String name, String type) {
		return column(name, type, null, null);
	}

	public CreateTableBuilder partitionColumn(String name, String type) {
		return column(name, type, PARTITION, null);
	}

	public CreateTableBuilder primaryKeyColumn(String name, String type) {
		return primaryKeyColumn(name, type, ASCENDING);
	}

	public CreateTableBuilder primaryKeyColumn(String name, String type, Ordering order) {
		return column(name, type, PRIMARY, order);
	}

	protected CreateTableBuilder column(String name, String type, KeyType key, Ordering order) {
		columns().add(new ColumnBuilder().name(name).type(type).keyType(key).ordering(order));
		return this;
	}

	/**
	 * Convenient method that calls <code>with("COMPACT STORAGE", null)</code>.
	 * 
	 * @see #with(String, Object)
	 * @return this
	 */
	public CreateTableBuilder withCompactStorage() {
		return with("COMPACT STORAGE");
	}

	protected List<ColumnBuilder> columns() {
		return columns == null ? columns = new ArrayList<ColumnBuilder>() : columns;
	}

	protected Map<String, Object> options() {
		return options == null ? options = new HashMap<String, Object>() : options;
	}

	public String toCql() {

		StringBuilder cql = new StringBuilder();

		preambleCql(cql);
		columnsAndOptionsCql(cql);

		cql.append(";");

		return cql.toString();
	}

	protected StringBuilder preambleCql(StringBuilder cql) {
		return (cql = ensureNotNull(cql)).append("CREATE TABLE ").append(ifNotExists ? "IF NOT EXISTS " : "").append(name);
	}

	@SuppressWarnings("unchecked")
	protected StringBuilder columnsAndOptionsCql(StringBuilder cql) {

		cql = ensureNotNull(cql);

		// begin columns
		cql.append(" (");

		List<ColumnBuilder> partitionKeys = new ArrayList<ColumnBuilder>();
		List<ColumnBuilder> primaryKeys = new ArrayList<ColumnBuilder>();
		for (ColumnBuilder col : columns) {
			col.toCql(cql).append(", ");

			if (col.getKeyType() == PARTITION) {
				partitionKeys.add(col);
			} else if (col.getKeyType() == PRIMARY) {
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

		boolean first = true;
		for (ColumnBuilder col : partitionKeys) {
			if (first) {
				first = false;
			} else {
				partitions.append(", ");
			}
			partitions.append(col.getName());

		}
		if (partitionKeys.size() > 1) {
			partitions.append(")");
		}

		StringBuilder clustering = null;
		boolean clusteringFirst = true;
		first = true;
		for (ColumnBuilder col : primaryKeys) {
			if (first) {
				first = false;
			} else {
				primaries.append(", ");
			}
			primaries.append(col.getName());

			if (col.getOrdering() != null) { // then ordering specified
				if (clustering == null) { // then initialize clustering clause
					clustering = new StringBuilder().append("CLUSTERING ORDER BY (");
				}
				if (clusteringFirst) {
					clusteringFirst = false;
				} else {
					clustering.append(", ");
				}
				clustering.append(col.getName()).append(" ").append(col.getOrdering().cql());
			}
		}
		if (clustering != null) { // then end clustering option
			clustering.append(")");
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

					cql.append(" = ");

					Map<String, Object> valueMap = null;
					if ((value instanceof Map) && !(valueMap = (Map<String, Object>) value).isEmpty()) {
						// then option value is a non-empty map

						// append { 'name' : 'value', ... }
						cql.append("{ ");
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
						cql.append(" }");

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

	@Override
	public String toString() {
		return toCql();
	}
}
