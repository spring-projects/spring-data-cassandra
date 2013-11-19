package org.springframework.data.cassandra.cql.builder;

import static org.springframework.data.cassandra.cql.CqlStringUtils.noNull;
import static org.springframework.data.cassandra.mapping.KeyType.PARTITION;
import static org.springframework.data.cassandra.mapping.KeyType.PRIMARY;
import static org.springframework.data.cassandra.mapping.Ordering.ASCENDING;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.data.cassandra.mapping.KeyType;
import org.springframework.data.cassandra.mapping.Ordering;

import com.datastax.driver.core.DataType;

/**
 * Builder class to construct CQL for a <code>CREATE TABLE</code> statement. Not threadsafe.
 * 
 * @author Matthew T. Adams
 */
public class CreateTableBuilder extends AbstractTableBuilder<CreateTableBuilder> {

	private boolean ifNotExists = false;
	private List<ColumnBuilder> columns = new ArrayList<ColumnBuilder>();

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

	public CreateTableBuilder column(String name, DataType type) {
		return column(name, type, null, null);
	}

	public CreateTableBuilder partitionKeyColumn(String name, DataType type) {
		return column(name, type, PARTITION, null);
	}

	public CreateTableBuilder primaryKeyColumn(String name, DataType type) {
		return primaryKeyColumn(name, type, ASCENDING);
	}

	public CreateTableBuilder primaryKeyColumn(String name, DataType type, Ordering order) {
		return column(name, type, PRIMARY, order);
	}

	protected CreateTableBuilder column(String name, DataType type, KeyType keyType, Ordering ordering) {
		columns().add(new ColumnBuilder().name(name).type(type).keyType(keyType).ordering(ordering));
		return this;
	}

	protected List<ColumnBuilder> columns() {
		return columns == null ? columns = new ArrayList<ColumnBuilder>() : columns;
	}

	public StringBuilder toCql(StringBuilder cql) {

		cql = noNull(cql);

		preambleCql(cql);
		columnsAndOptionsCql(cql);

		cql.append(";");

		return cql;
	}

	protected StringBuilder preambleCql(StringBuilder cql) {
		return noNull(cql).append("CREATE TABLE ").append(ifNotExists ? "IF NOT EXISTS " : "")
				.append(getNameAsIdentifier());
	}

	@SuppressWarnings("unchecked")
	protected StringBuilder columnsAndOptionsCql(StringBuilder cql) {

		cql = noNull(cql);

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
			// end option preamble

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
