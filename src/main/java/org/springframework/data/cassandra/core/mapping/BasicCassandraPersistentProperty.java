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
package org.springframework.data.cassandra.core.mapping;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.model.AnnotationBasedPersistentProperty;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * CassandraDB specific {@link org.springframework.data.mapping.CassandraPersistentProperty} implementation.
 * 
 * @author Oliver Gierke
 * @author Patryk Wasik
 */
public class BasicCassandraPersistentProperty extends AnnotationBasedPersistentProperty<CassandraPersistentProperty> implements
		CassandraPersistentProperty {

	private static final Logger LOG = LoggerFactory.getLogger(BasicCassandraPersistentProperty.class);

	private static final String ID_FIELD_NAME = "_id";
	private static final Set<Class<?>> SUPPORTED_ID_TYPES = new HashSet<Class<?>>();
	private static final Set<String> SUPPORTED_ID_PROPERTY_NAMES = new HashSet<String>();

	private static final Field CAUSE_FIELD;

	static {
		SUPPORTED_ID_TYPES.add(String.class);
		SUPPORTED_ID_TYPES.add(BigInteger.class);

		SUPPORTED_ID_PROPERTY_NAMES.add("id");
		SUPPORTED_ID_PROPERTY_NAMES.add("_id");

		CAUSE_FIELD = ReflectionUtils.findField(Throwable.class, "cause");
	}

	private final ColumnNamingStrategy fieldNamingStrategy;

	/**
	 * Creates a new {@link BasicCassandraPersistentProperty}.
	 * 
	 * @param field
	 * @param propertyDescriptor
	 * @param owner
	 * @param simpleTypeHolder
	 * @param fieldNamingStrategy
	 */
	public BasicCassandraPersistentProperty(Field field, PropertyDescriptor propertyDescriptor,
			CassandraPersistentEntity<?> owner, SimpleTypeHolder simpleTypeHolder, ColumnNamingStrategy fieldNamingStrategy) {

		super(field, propertyDescriptor, owner, simpleTypeHolder);
		this.fieldNamingStrategy = fieldNamingStrategy == null ? PropertyNameColumnNamingStrategy.INSTANCE
				: fieldNamingStrategy;

		if (isIdProperty() && getFieldName() != ID_FIELD_NAME) {
			LOG.warn("Customizing field name for id property not allowed! Custom name will not be considered!");
		}
	}

	/**
	 * Also considers fields as id that are of supported id type and name.
	 * 
	 * @see #SUPPORTED_ID_PROPERTY_NAMES
	 * @see #SUPPORTED_ID_TYPES
	 */
	@Override
	public boolean isIdProperty() {

		if (super.isIdProperty()) {
			return true;
		}

		// We need to support a wider range of ID types than just the ones that can be converted to an ObjectId
		return SUPPORTED_ID_PROPERTY_NAMES.contains(field.getName());
	}

	/**
	 * Returns the key to be used to store the value of the property inside a Cassandra {@link DBObject}.
	 * 
	 * @return
	 */
	public String getFieldName() {

		if (isIdProperty()) {
			return ID_FIELD_NAME;
		}

		org.springframework.data.cassandra.core.mapping.Column annotation = findAnnotation(org.springframework.data.cassandra.core.mapping.Column.class);

		if (annotation != null && StringUtils.hasText(annotation.value())) {
			return annotation.value();
		}

		String fieldName = fieldNamingStrategy.getColumnName(this);

		if (!StringUtils.hasText(fieldName)) {
			throw new MappingException(String.format("Invalid (null or empty) field name returned for property %s by %s!",
					this, fieldNamingStrategy.getClass()));
		}

		return fieldName;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty#getFieldOrder()
	 */
	public int getFieldOrder() {
		org.springframework.data.cassandra.core.mapping.Column annotation = getField().getAnnotation(
				org.springframework.data.cassandra.core.mapping.Column.class);
		return annotation != null ? annotation.order() : Integer.MAX_VALUE;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.model.AbstractPersistentProperty#createAssociation()
	 */
	@Override
	protected Association<CassandraPersistentProperty> createAssociation() {
		return new Association<CassandraPersistentProperty>(this, null);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty#usePropertyAccess()
	 */
	public boolean usePropertyAccess() {
		return CAUSE_FIELD.equals(getField());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty#getColumnName()
	 */
	public String getColumnName() {
		// TODO Auto-generated method stub
		return null;
	}
}
