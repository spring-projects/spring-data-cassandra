/*******************************************************************************
 * Copyright 2013-2014 the original author or authors.
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
 ******************************************************************************/
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
