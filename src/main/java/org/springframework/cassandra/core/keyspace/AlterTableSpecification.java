package org.springframework.cassandra.core.keyspace;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.datastax.driver.core.DataType;

public class AlterTableSpecification extends AbstractTableSpecification<AlterTableSpecification> {

	private List<ColumnChangeSpecification> changes = new ArrayList<ColumnChangeSpecification>();

	public AlterTableSpecification drop(String column) {
		changes.add(new DropColumnSpecification(column));
		return this;
	}

	public AlterTableSpecification add(String column, DataType type) {
		changes.add(new AddColumnSpecification(column, type));
		return this;
	}

	public AlterTableSpecification alter(String column, DataType type) {
		changes.add(new AlterColumnSpecification(column, type));
		return this;
	}

	public List<ColumnChangeSpecification> getChanges() {
		return Collections.unmodifiableList(changes);
	}
}
