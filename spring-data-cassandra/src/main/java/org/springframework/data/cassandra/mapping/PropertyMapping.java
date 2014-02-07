package org.springframework.data.cassandra.mapping;

import org.springframework.util.Assert;

/**
 * Mapping between a persistent entity's property and its column.
 * 
 * @author Matthew T. Adams
 */
public class PropertyMapping {

	protected String propertyName;
	protected String columnName;

	public PropertyMapping(String propertyName, String columnName) {

		setPropertyName(propertyName);
		setColumnName(columnName);
	}

	public String getPropertyName() {
		return propertyName;
	}

	protected void setPropertyName(String propertyName) {
		Assert.notNull(propertyName);
		this.propertyName = propertyName;
	}

	public String getColumnName() {
		return columnName;
	}

	protected void setColumnName(String columnName) {
		Assert.notNull(columnName);
		this.columnName = columnName;
	}
}
