package org.springframework.cassandra.core.cql.generator;

import static org.springframework.cassandra.core.cql.CqlStringUtils.noNull;
import static org.springframework.data.cassandra.mapping.KeyType.PARTITION;
import static org.springframework.data.cassandra.mapping.KeyType.PRIMARY;

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
 */
public class CreateTableCqlGenerator extends AbstractTableOperationCqlGenerator<CreateTableSpecification> {

	public CreateTableCqlGenerator(CreateTableSpecification specification) {
		super(specification);
	}

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
		List<ColumnSpecification> primaryKeys = new ArrayList<ColumnSpecification>();
		for (ColumnSpecification col : spec().getColumns()) {
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
		for (ColumnSpecification col : partitionKeys) {
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
		for (ColumnSpecification col : primaryKeys) {
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
		Map<String, Object> options = spec().getOptions();

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
