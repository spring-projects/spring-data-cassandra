/*
 * Copyright 2011-2012 the original author or authors.
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
package org.springframework.data.cassandra.mapping;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.List;

import org.springframework.cassandra.core.Ordering;
import org.springframework.data.util.TypeInformation;

import com.datastax.driver.core.DataType;

/**
 * {@link BasicCassandraPersistentProperty} subclass that caches call results from the superclass.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 */
public class CachingCassandraPersistentProperty extends BasicCassandraPersistentProperty {

	private Boolean isIdProperty;
	private Boolean isIndexed;
	private Boolean isCompositePrimaryKey;
	private Boolean isPartitionKeyColumn;
	private Boolean isClusterKeyColumn;
	private Boolean isPrimaryKeyColumn;
	private String columnName;
	private List<String> columnNames;
	private Ordering ordering;
	private boolean orderingCached = false;
	private DataType dataType;
	private Class<?> compositePrimaryKeyType;
	private TypeInformation<?> compositePrimaryKeyTypeInformation;

	/**
	 * Creates a new {@link CachingCassandraPersistentProperty}.
	 */
	public CachingCassandraPersistentProperty(Field field, PropertyDescriptor propertyDescriptor,
			CassandraPersistentEntity<?> owner, CassandraSimpleTypeHolder simpleTypeHolder) {
		super(field, propertyDescriptor, owner, simpleTypeHolder);
	}

	@Override
	public TypeInformation<?> getCompositePrimaryKeyTypeInformation() {

		if (compositePrimaryKeyTypeInformation == null) {
			compositePrimaryKeyTypeInformation = super.getCompositePrimaryKeyTypeInformation();
		}
		return compositePrimaryKeyTypeInformation;
	}

	@Override
	public Class<?> getCompositePrimaryKeyType() {

		if (compositePrimaryKeyType == null) {
			compositePrimaryKeyType = super.getCompositePrimaryKeyType();
		}
		return compositePrimaryKeyType;
	}

	@Override
	public boolean isClusterKeyColumn() {

		if (isClusterKeyColumn == null) {
			isClusterKeyColumn = super.isClusterKeyColumn();
		}
		return isClusterKeyColumn;
	}

	@Override
	public boolean isPrimaryKeyColumn() {

		if (isPrimaryKeyColumn == null) {
			isPrimaryKeyColumn = super.isPrimaryKeyColumn();
		}
		return isPrimaryKeyColumn;
	}

	@Override
	public DataType getDataType() {

		if (dataType == null) {
			dataType = super.getDataType();
		}
		return dataType;
	}

	@Override
	public Ordering getPrimaryKeyOrdering() {

		if (!orderingCached) {
			ordering = super.getPrimaryKeyOrdering();
			orderingCached = true;
		}
		return ordering;
	}

	@Override
	public boolean isCompositePrimaryKey() {

		if (isCompositePrimaryKey == null) {
			isCompositePrimaryKey = super.isCompositePrimaryKey();
		}
		return isCompositePrimaryKey;
	}

	@Override
	public boolean isIdProperty() {

		if (isIdProperty == null) {
			isIdProperty = super.isIdProperty();
		}
		return isIdProperty;
	}

	@Override
	public String getColumnName() {

		if (columnName == null) {
			columnName = super.getColumnName();
		}
		return columnName;
	}

	@Override
	public boolean isIndexed() {

		if (isIndexed == null) {
			isIndexed = super.isIndexed();
		}
		return isIndexed;
	}

	@Override
	public boolean isPartitionKeyColumn() {

		if (isPartitionKeyColumn == null) {
			isPartitionKeyColumn = super.isPartitionKeyColumn();
		}
		return isPartitionKeyColumn;
	}

	@Override
	public List<String> getColumnNames() {
		if (columnNames == null) {
			columnNames = super.getColumnNames();
		}
		return columnNames;
	}
}
