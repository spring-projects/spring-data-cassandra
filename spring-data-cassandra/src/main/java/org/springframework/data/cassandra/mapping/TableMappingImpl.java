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

import java.util.HashSet;
import java.util.Set;

import org.springframework.util.Assert;

/**
 * Default implementation of {@link TableMapping}.
 * 
 * @author Matthew T. Adams
 */
public class TableMappingImpl implements TableMapping {

	private Set<Class<?>> entityClasses = new HashSet<Class<?>>();
	private String tableName;

	public TableMappingImpl(Class<?> entityClass, String tableName) {
		this(tableName);
		setEntityClass(entityClass);
	}

	public TableMappingImpl(Set<Class<?>> entityClasses, String tableName) {
		this(tableName);
		setEntityClasses(entityClasses);
	}

	protected TableMappingImpl(String tableName) {
		setTableName(tableName);
	}

	@Override
	public Class<?> getEntityClass() {
		if (entityClasses.size() != 1) {
			throw new IllegalStateException("more than one entity class exists in this TableMapping");
		}
		return entityClasses.iterator().next();
	}

	@Override
	public Set<Class<?>> getEntityClasses() {
		return entityClasses;
	}

	@Override
	public void setEntityClasses(Set<Class<?>> entityClasses) {
		this.entityClasses = entityClasses == null ? new HashSet<Class<?>>() : new HashSet<Class<?>>(entityClasses);
	}

	@Override
	public String getTableName() {
		return tableName;
	}

	@Override
	public void setTableName(String tableName) {
		Assert.notNull(tableName);
		this.tableName = tableName;
	}

	@Override
	public void setEntityClass(Class<?> entityClass) {
		if (entityClass == null) {
			throw new IllegalArgumentException("entity class required");
		}

		entityClasses.clear();
		entityClasses.add(entityClass);
	}

	@Override
	public boolean equals(Object that) {

		if (this == that) {
			return true;
		}
		if (that == null) {
			return false;
		}
		if (!(that instanceof TableMapping)) {
			return false;
		}

		TableMapping thatMapping = (TableMapping) that;

		if (!this.tableName.equals(thatMapping.getTableName())) {
			return false;
		}

		return this.entityClasses.equals(thatMapping.getEntityClasses());
	}

	@Override
	public int hashCode() {
		return tableName.hashCode() ^ entityClasses.hashCode();
	}
}
