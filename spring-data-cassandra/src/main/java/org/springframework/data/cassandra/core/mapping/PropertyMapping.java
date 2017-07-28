/*
 * Copyright 2013-2017 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Mapping between a persistent entity's property and its column.
 *
 * @author Matthew T. Adams
 * @author John Blum
 */
public class PropertyMapping {

	private @Nullable String columnName;

	private @Nullable String forceQuote;

	private String propertyName;

	public PropertyMapping(String propertyName) {

		Assert.notNull(propertyName, "Property name must not be null");

		this.propertyName = propertyName;
	}

	public PropertyMapping(String propertyName, String columnName) {
		this(propertyName, columnName, "false");
	}

	public PropertyMapping(String propertyName, String columnName, String forceQuote) {

		Assert.notNull(propertyName, "Property name must not be null");
		this.propertyName = propertyName;

		setColumnName(columnName);
		setForceQuote(forceQuote);
	}

	@Nullable
	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {

		Assert.notNull(columnName, "Column name must not be null");
		this.columnName = columnName;
	}

	@Nullable
	public String getForceQuote() {
		return forceQuote;
	}

	public void setForceQuote(String forceQuote) {
		this.forceQuote = forceQuote;
	}

	public String getPropertyName() {
		return propertyName;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (!(obj instanceof PropertyMapping)) {
			return false;
		}

		PropertyMapping that = (PropertyMapping) obj;

		return ObjectUtils.nullSafeEquals(this.getPropertyName(), that.getPropertyName())
				&& ObjectUtils.nullSafeEquals(this.getColumnName(), that.getColumnName())
				&& ObjectUtils.nullSafeEquals(this.getForceQuote(), that.getForceQuote());
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {

		int hashValue = 17;
		hashValue = 37 * hashValue + ObjectUtils.nullSafeHashCode(this.getPropertyName());
		hashValue = 37 * hashValue + ObjectUtils.nullSafeHashCode(this.getColumnName());
		hashValue = 37 * hashValue + ObjectUtils.nullSafeHashCode(this.getForceQuote());
		return hashValue;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("{ @type = %1$s, propertyName = %2$s, columnName = %3$s, forceQuote = %4$s }",
				getClass().getName(), getPropertyName(), getColumnName(), getForceQuote());
	}
}
