package org.springframework.cassandra.core.keyspace;

import static org.springframework.data.cassandra.mapping.KeyType.PARTITION;
import static org.springframework.data.cassandra.mapping.KeyType.PRIMARY;
import static org.springframework.data.cassandra.mapping.Ordering.ASCENDING;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.data.cassandra.mapping.KeyType;
import org.springframework.data.cassandra.mapping.Ordering;

import com.datastax.driver.core.DataType;

/**
 * Builder class to construct CQL for a <code>CREATE TABLE</code> statement. Not threadsafe.
 * 
 * @author Matthew T. Adams
 */
public class CreateTableSpecification extends AbstractTableSpecification<CreateTableSpecification> {

	private boolean ifNotExists = false;
	private List<ColumnSpecification> columns = new ArrayList<ColumnSpecification>();

	/**
	 * Causes the inclusion of an <code>IF NOT EXISTS</code> clause.
	 * 
	 * @return this
	 */
	public CreateTableSpecification ifNotExists() {
		return ifNotExists(true);
	}

	/**
	 * Toggles the inclusion of an <code>IF NOT EXISTS</code> clause.
	 * 
	 * @return this
	 */
	public CreateTableSpecification ifNotExists(boolean ifNotExists) {
		this.ifNotExists = ifNotExists;
		return this;
	}

	public CreateTableSpecification column(String name, DataType type) {
		return column(name, type, null, null);
	}

	public CreateTableSpecification partitionKeyColumn(String name, DataType type) {
		return column(name, type, PARTITION, null);
	}

	public CreateTableSpecification primaryKeyColumn(String name, DataType type) {
		return primaryKeyColumn(name, type, ASCENDING);
	}

	public CreateTableSpecification primaryKeyColumn(String name, DataType type, Ordering order) {
		return column(name, type, PRIMARY, order);
	}

	protected CreateTableSpecification column(String name, DataType type, KeyType keyType, Ordering ordering) {
		columns().add(new ColumnSpecification().name(name).type(type).keyType(keyType).ordering(ordering));
		return this;
	}

	protected List<ColumnSpecification> columns() {
		return columns == null ? columns = new ArrayList<ColumnSpecification>() : columns;
	}

	public boolean getIfNotExists() {
		return ifNotExists;
	}

	public List<ColumnSpecification> getColumns() {
		return Collections.unmodifiableList(columns);
	}
}
