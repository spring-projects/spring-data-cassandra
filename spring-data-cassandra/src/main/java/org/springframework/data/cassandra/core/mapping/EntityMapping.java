/*
 * Copyright 2013-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Mapping information for an individual entity class.
 *
 * @author Matthew T. Adams
 * @author John Blum
 */
public class EntityMapping {

	/**
	 * The {@link PropertyMapping}s for each persistent property, keyed on property name.
	 */
	private Map<String, PropertyMapping> propertyMappings = Collections.emptyMap();

	/**
	 * The name of the entity's class.
	 */
	private @Nullable String entityClassName;

	/**
	 * Whether to force the table name to be quoted.
	 *
	 * @deprecated since 3.0. The table name gets converted into {@link com.datastax.oss.driver.api.core.CqlIdentifier}
	 *             hence it no longer requires an indication whether the name should be quoted.
	 */
	private @Deprecated String forceQuote = "false";

	/**
	 * The name of the table to which the entity is mapped.
	 */
	private String tableName = "";

	public EntityMapping(String entityClassName, String tableName) {
		this(entityClassName, tableName, Boolean.FALSE.toString());
	}

	public EntityMapping(String entityClassName, String tableName, String forceQuote) {
		setEntityClassName(entityClassName);
		setTableName(tableName);
		setForceQuote(forceQuote);
	}

	@Nullable
	public String getEntityClassName() {
		return this.entityClassName;
	}

	public void setEntityClassName(String entityClassName) {

		Assert.hasText(entityClassName, "Entity class name must not be null or empty");

		this.entityClassName = entityClassName;
	}

	/**
	 * @return
	 * @deprecated since 3.0. The type name gets converted into {@link com.datastax.oss.driver.api.core.CqlIdentifier}
	 *             hence it no longer requires an indication whether the name should be quoted.
	 */
	@Deprecated
	public String getForceQuote() {
		return this.forceQuote;
	}

	@Deprecated
	public void setForceQuote(String forceQuote) {

		Assert.notNull(forceQuote, "Force quote must not be null or empty");

		this.forceQuote = forceQuote;
	}

	public Map<String, PropertyMapping> getPropertyMappings() {
		return Collections.unmodifiableMap(this.propertyMappings);
	}

	public void setPropertyMappings(@Nullable Map<String, PropertyMapping> propertyMappings) {
		this.propertyMappings = propertyMappings != null ? new HashMap<>(propertyMappings) : Collections.emptyMap();
	}

	public String getTableName() {
		return this.tableName;
	}

	public void setTableName(String tableName) {

		Assert.notNull(tableName, "Table name must not be null or empty");

		this.tableName = tableName;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {

		if (this == obj) {
			return true;
		}

		if (!(obj instanceof EntityMapping)) {
			return false;
		}

		EntityMapping that = (EntityMapping) obj;

		return ObjectUtils.nullSafeEquals(this.getEntityClassName(), that.getEntityClassName())
				&& ObjectUtils.nullSafeEquals(this.getForceQuote(), that.getForceQuote())
				&& ObjectUtils.nullSafeEquals(this.getTableName(), that.getTableName());
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {

		int hashValue = 17;

		hashValue = 37 * hashValue + ObjectUtils.nullSafeHashCode(this.getEntityClassName());
		hashValue = 37 * hashValue + ObjectUtils.nullSafeHashCode(this.getForceQuote());
		hashValue = 37 * hashValue + ObjectUtils.nullSafeHashCode(this.getTableName());

		return hashValue;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format(
				"{ @type = %1$s, entityClassName = %2$s, tableName = %3$s, forceQuote = %4$s, propertyMappings = %5$s }",
				getClass().getName(), getEntityClassName(), getTableName(), getForceQuote(), toString(getPropertyMappings()));
	}

	private String toString(Map<?, ?> map) {

		StringBuilder builder = new StringBuilder("[");

		int count = 0;

		for (Map.Entry<?, ?> entry : map.entrySet()) {
			builder.append(++count > 1 ? ", " : "");
			builder.append(String.format("%1$s = %2$s", entry.getKey(), entry.getValue()));
		}

		return builder.toString();
	}
}
