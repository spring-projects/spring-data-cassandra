/*
 * Copyright 2013-2014 the original author or authors
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
package org.springframework.data.cassandra.mapping;

import static org.springframework.cassandra.core.cql.CqlIdentifier.cqlId;
import static org.springframework.cassandra.core.cql.CqlIdentifier.quotedCqlId;

import org.springframework.util.Assert;

/**
 * Mapping between a persistent entity's property and its column.
 * 
 * @author Matthew T. Adams
 */
public class PropertyMapping {

	protected String propertyName;
	protected String columnName;
	protected String forceQuote;

	public PropertyMapping(String propertyName) {
		setPropertyName(propertyName);
	}

	public PropertyMapping(String propertyName, String columnName) {
		this(propertyName, columnName, "false");
	}

	public PropertyMapping(String propertyName, String columnName, String forceQuote) {

		setPropertyName(propertyName);
		setColumnName(columnName);
		setForceQuote(forceQuote);
	}

	public String getPropertyName() {
		return propertyName;
	}

	public void setPropertyName(String propertyName) {
		Assert.notNull(propertyName);
		this.propertyName = propertyName;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		Assert.notNull(columnName);
		this.columnName = columnName;
	}

	public String getForceQuote() {
		return forceQuote;
	}

	public void setForceQuote(String forceQuote) {
		this.forceQuote = forceQuote;
	}

	@Override
	public boolean equals(Object that) {

		if (this == that) {
			return true;
		}
		if (that == null) {
			return false;
		}
		if (!(that instanceof PropertyMapping)) {
			return false;
		}

		PropertyMapping other = (PropertyMapping) that;

		if (this.propertyName == null) {
			if (other.propertyName != null) {
				return false;
			}
		} else if (!this.propertyName.equals(other.propertyName)) {
			return false;
		}

		if (this.columnName == null) {
			if (other.columnName != null) {
				return false;
			}
		} else if (!this.columnName.equals(other.columnName)) {
			return false;
		}

		if (this.forceQuote == null) {
			if (other.forceQuote != null) {
				return false;
			}
		} else if (this.forceQuote.equals(other.forceQuote)) {
			return false;
		}

		return true;
	}

	@Override
	public int hashCode() {
		int hashCode = 37;
		hashCode ^= propertyName == null ? 0 : propertyName.hashCode();
		hashCode ^= columnName == null ? 0 : columnName.hashCode();
		hashCode ^= forceQuote == null ? 0 : forceQuote.hashCode();
		return hashCode;
	}
}
