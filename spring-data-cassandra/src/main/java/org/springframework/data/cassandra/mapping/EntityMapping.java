package org.springframework.data.cassandra.mapping;

import java.util.HashMap;
import java.util.Map;

/**
 * Mapping information for an individual entity class.
 * 
 * @author Matthew T. Adams
 */
public class EntityMapping {

	/**
	 * The name of the entity's class.
	 */
	protected String entityClassName;

	/**
	 * The name of the table to which the entity is mapped.
	 */
	protected String tableName;

	/**
	 * The {@link PropertyMapping}s for each persistent property, keyed on property name.
	 */
	protected Map<String, PropertyMapping> propertyMappings = new HashMap<String, PropertyMapping>();

	public EntityMapping(String entityClassName, String tableName) {
		setEntityClassName(entityClassName);
		setTableName(tableName);
	}

	public String getEntityClassName() {
		return entityClassName;
	}

	public void setEntityClassName(String entityClassName) {
		this.entityClassName = entityClassName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	@Override
	public boolean equals(Object that) {
		if (that == null) {
			return false;
		}
		if (this == that) {
			return true;
		}
		if (!(that instanceof EntityMapping)) {
			return false;
		}

		EntityMapping thatMapping = (EntityMapping) that;

		return this.entityClassName.equals(thatMapping.entityClassName) && this.tableName.equals(thatMapping.tableName);
	}

	@Override
	public int hashCode() {
		return entityClassName.hashCode() ^ tableName.hashCode();
	}
}
