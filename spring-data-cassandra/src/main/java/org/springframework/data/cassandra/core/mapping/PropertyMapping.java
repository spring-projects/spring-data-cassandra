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

import org.springframework.lang.NonNull;
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

	/**
	 * @deprecated since 3.0. The column name gets converted into {@link com.datastax.oss.driver.api.core.CqlIdentifier}
	 *             hence it no longer requires an indication whether the name should be quoted.
	 * @see com.datastax.oss.driver.api.core.CqlIdentifier#fromInternal(String)
	 */
	@Deprecated
	private @Nullable String forceQuote;

	private String propertyName;

	public PropertyMapping(String propertyName) {

		Assert.notNull(propertyName, "Property name must not be null");

		this.propertyName = propertyName;
	}

	public PropertyMapping(String propertyName, String columnName) {
		this(propertyName, columnName, "false");
	}

	public PropertyMapping(@NonNull String propertyName, @NonNull String columnName, String forceQuote) {

		Assert.notNull(propertyName, "Property name must not be null");

		this.propertyName = propertyName;

		setColumnName(columnName);
		setForceQuote(forceQuote);
	}

	@Nullable
	public String getColumnName() {
		return this.columnName;
	}

	public void setColumnName(@NonNull String columnName) {

		Assert.notNull(columnName, "Column name must not be null");

		this.columnName = columnName;
	}

	@Nullable
	@Deprecated
	public String getForceQuote() {
		return this.forceQuote;
	}

	@Deprecated
	public void setForceQuote(String forceQuote) {
		this.forceQuote = forceQuote;
	}

	public String getPropertyName() {
		return this.propertyName;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o) {

		if (this == o) {
			return true;
		}

		if (!(o instanceof PropertyMapping)) {
			return false;
		}

		PropertyMapping that = (PropertyMapping) o;

		if (!ObjectUtils.nullSafeEquals(columnName, that.columnName)) {
			return false;
		}

		if (!ObjectUtils.nullSafeEquals(forceQuote, that.forceQuote)) {
			return false;
		}

		return ObjectUtils.nullSafeEquals(propertyName, that.propertyName);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(columnName);
		result = 31 * result + ObjectUtils.nullSafeHashCode(forceQuote);
		result = 31 * result + ObjectUtils.nullSafeHashCode(propertyName);
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("{ @type = %1$s, propertyName = %2$s, columnName = %3$s, forceQuote = %4$s }",
				getClass().getName(), getPropertyName(), getColumnName(), getForceQuote());
	}
}
