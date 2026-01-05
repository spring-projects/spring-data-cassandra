/*
 * Copyright 2024-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * {@link CassandraPersistentProperty} wrapper for a delegate {@link CassandraPersistentProperty} considering
 * annotations from a constructor parameter.
 *
 * @author Mark Paluch
 * @since 3.2
 */
class AnnotatedCassandraConstructorProperty implements CassandraPersistentProperty {

	private final CassandraPersistentProperty delegate;

	private final MergedAnnotation<Column> column;

	private final MergedAnnotation<Element> element;

	public AnnotatedCassandraConstructorProperty(CassandraPersistentProperty delegate, MergedAnnotations annotations) {
		this.delegate = delegate;
		this.column = annotations.get(Column.class);
		this.element = annotations.get(Element.class);
	}

	@Override
	@Nullable
	public CqlIdentifier getColumnName() {
		return column.isPresent() ? CqlIdentifier.fromCql(column.getString("value")) : delegate.getColumnName();
	}

	@Override
	public boolean hasExplicitColumnName() {
		return column.isPresent() && !ObjectUtils.isEmpty(column.getString("value"));
	}

	@Override
	@Nullable
	public Integer getOrdinal() {
		return element.isPresent() ? Integer.valueOf(element.getInt("value")) : delegate.getOrdinal();
	}

	@Override
	@Nullable
	public Ordering getPrimaryKeyOrdering() {
		return delegate.getPrimaryKeyOrdering();
	}

	@Override
	public boolean isClusterKeyColumn() {
		return delegate.isClusterKeyColumn();
	}

	@Override
	public boolean isCompositePrimaryKey() {
		return delegate.isCompositePrimaryKey();
	}

	@Override
	public boolean isMapLike() {
		return delegate.isMapLike();
	}

	@Override
	public boolean isPartitionKeyColumn() {
		return delegate.isPartitionKeyColumn();
	}

	@Override
	public boolean isPrimaryKeyColumn() {
		return delegate.isPrimaryKeyColumn();
	}

	@Override
	public boolean isStaticColumn() {
		return delegate.isStaticColumn();
	}

	@Override
	@Nullable
	public AnnotatedType findAnnotatedType(Class<? extends Annotation> annotationType) {
		return delegate.findAnnotatedType(annotationType);
	}

	@Override
	public PersistentEntity<?, CassandraPersistentProperty> getOwner() {
		return delegate.getOwner();
	}

	@Override
	public String getName() {
		return delegate.getName();
	}

	@Override
	public Class<?> getType() {
		return delegate.getType();
	}

	@Override
	public TypeInformation<?> getTypeInformation() {
		return delegate.getTypeInformation();
	}

	@Override
	@Nullable
	public Method getGetter() {
		return delegate.getGetter();
	}

	@Override
	@Nullable
	public Method getSetter() {
		return delegate.getSetter();
	}

	@Override
	@Nullable
	public Method getWither() {
		return delegate.getWither();
	}

	@Override
	@Nullable
	public Field getField() {
		return delegate.getField();
	}

	@Override
	@Nullable
	public String getSpelExpression() {
		return delegate.getSpelExpression();
	}

	@Override
	@Nullable
	public Association<CassandraPersistentProperty> getAssociation() {
		return delegate.getAssociation();
	}

	@Override
	public boolean isEntity() {
		return delegate.isEntity();
	}

	@Override
	public boolean isIdProperty() {
		return delegate.isIdProperty();
	}

	@Override
	public boolean isVersionProperty() {
		return delegate.isVersionProperty();
	}

	@Override
	public boolean isCollectionLike() {
		return delegate.isCollectionLike();
	}

	@Override
	public boolean isMap() {
		return delegate.isMap();
	}

	@Override
	public boolean isArray() {
		return delegate.isArray();
	}

	@Override
	public boolean isTransient() {
		return delegate.isTransient();
	}

	@Override
	public boolean isWritable() {
		return delegate.isWritable();
	}

	@Override
	public boolean isReadable() {
		return delegate.isReadable();
	}

	@Override
	public boolean isImmutable() {
		return delegate.isImmutable();
	}

	@Override
	public boolean isAssociation() {
		return delegate.isAssociation();
	}

	@Override
	@Nullable
	public Class<?> getComponentType() {
		return delegate.getComponentType();
	}

	@Override
	public Class<?> getRawType() {
		return delegate.getRawType();
	}

	@Override
	@Nullable
	public Class<?> getMapValueType() {
		return delegate.getMapValueType();
	}

	@Override
	public Class<?> getActualType() {
		return delegate.getActualType();
	}

	@Override
	@Nullable
	public <A extends Annotation> A findAnnotation(Class<A> annotationType) {
		return delegate.findAnnotation(annotationType);
	}

	@Override
	@Nullable
	public <A extends Annotation> A findPropertyOrOwnerAnnotation(Class<A> annotationType) {
		return delegate.findPropertyOrOwnerAnnotation(annotationType);
	}

	@Override
	public boolean isAnnotationPresent(Class<? extends Annotation> annotationType) {
		return delegate.isAnnotationPresent(annotationType);
	}

	@Override
	public boolean usePropertyAccess() {
		return delegate.usePropertyAccess();
	}

	@Override
	@Nullable
	public Class<?> getAssociationTargetType() {
		return delegate.getAssociationTargetType();
	}

	@Override
	public Iterable<? extends TypeInformation<?>> getPersistentEntityTypeInformation() {
		return delegate.getPersistentEntityTypeInformation();
	}

	@Nullable
	@Override
	public TypeInformation<?> getAssociationTargetTypeInformation() {
		return delegate.getAssociationTargetTypeInformation();
	}

	@Override
	public void setColumnName(CqlIdentifier columnName) {
		throw new UnsupportedOperationException();
	}

	@Override
	@Deprecated(forRemoval = true)
	public void setForceQuote(boolean forceQuote) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		throw new UnsupportedOperationException();
	}

}
