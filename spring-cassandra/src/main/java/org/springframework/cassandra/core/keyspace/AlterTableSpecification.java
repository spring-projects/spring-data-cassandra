package org.springframework.cassandra.core.keyspace;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.datastax.driver.core.DataType;

/**
 * Builder class to construct an <code>ALTER TABLE</code> specification.
 * 
 * @author Matthew T. Adams
 */
public class AlterTableSpecification extends TableOptionsSpecification<AlterTableSpecification> {

	/**
	 * The list of column changes.
	 */
	private List<ColumnChangeSpecification> changes = new ArrayList<ColumnChangeSpecification>();

	/**
	 * Adds a <code>DROP</code> to the list of column changes.
	 */
	public AlterTableSpecification drop(String column) {
		changes.add(new DropColumnSpecification(column));
		return this;
	}

	/**
	 * Adds an <code>ADD</code> to the list of column changes.
	 */
	public AlterTableSpecification add(String column, DataType type) {
		changes.add(new AddColumnSpecification(column, type));
		return this;
	}

	/**
	 * Adds an <code>ALTER</code> to the list of column changes.
	 */
	public AlterTableSpecification alter(String column, DataType type) {
		changes.add(new AlterColumnSpecification(column, type));
		return this;
	}

	/**
	 * Returns an unmodifiable list of column changes.
	 */
	public List<ColumnChangeSpecification> getChanges() {
		return Collections.unmodifiableList(changes);
	}
}
