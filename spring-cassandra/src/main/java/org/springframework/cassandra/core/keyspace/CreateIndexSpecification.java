/*
 * Copyright 2011-2013 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.core.keyspace;

import org.springframework.cassandra.core.CqlIdentifier;
import org.springframework.util.StringUtils;

/**
 * Builder class to construct a <code>CREATE INDEX</code> specification.
 * 
 * @author Matthew T. Adams
 * @author David Webb
 */
public class CreateIndexSpecification extends IndexNameSpecification<CreateIndexSpecification> implements
		IndexDescriptor {

	private boolean ifNotExists = false;
	private boolean custom = false;
	private CqlIdentifier identifier;
	private String columnName;
	private String using;

	/**
	 * Causes the inclusion of an <code>IF NOT EXISTS</code> clause.
	 * 
	 * @return this
	 */
	public CreateIndexSpecification ifNotExists() {
		return ifNotExists(true);
	}

	/**
	 * Toggles the inclusion of an <code>IF NOT EXISTS</code> clause.
	 * 
	 * @return this
	 */
	public CreateIndexSpecification ifNotExists(boolean ifNotExists) {
		this.ifNotExists = ifNotExists;
		return this;
	}

	public boolean getIfNotExists() {
		return ifNotExists;
	}

	public boolean isCustom() {
		return custom;
	}

	public CreateIndexSpecification using(String className) {

		if (StringUtils.hasText(className)) {
			this.using = className;
			this.custom = true;
		} else {
			this.using = null;
			this.custom = false;
		}

		return this;
	}

	public String getUsing() {
		return using;
	}

	public String getColumnName() {
		return columnName;
	}

	/**
	 * Sets the table name.
	 * 
	 * @return this
	 */
	public CreateIndexSpecification tableName(String tableName) {
		identifier = new CqlIdentifier(tableName);
		return this;
	}

	public String getTableName() {
		return identifier.getName();
	}

	public String getTableNameAsIdentifier() {
		return identifier.toCql();
	}

	public CreateIndexSpecification columnName(String columnName) {
		this.columnName = columnName;
		return this;
	}

	/**
	 * Entry point into the {@link CreateIndexSpecification}'s fluent API to create a index. Convenient if imported
	 * statically.
	 */
	public static CreateIndexSpecification createIndex() {
		return new CreateIndexSpecification();
	}

}
