/*
 * Copyright 2021 the original author or authors.
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
package org.springframework.data.cassandra.core.convert;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Optional;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Annotated {@link CassandraPersistentProperty} synthesized from a constructor parameter.
 *
 * @author Mark Paluch
 * @since 3.2
 */
class CassandraConstructorProperty implements CassandraPersistentProperty {

	private final String name;

	private final CassandraPersistentEntity<?> owner;

	private final TypeInformation<?> typeInformation;

	public CassandraConstructorProperty(String name, CassandraPersistentEntity<?> owner,
			TypeInformation<?> typeInformation) {
		this.name = name;
		this.owner = owner;
		this.typeInformation = typeInformation;
	}

	@Nullable
	@Override
	public CqlIdentifier getColumnName() {
		throw new IllegalStateException(String.format("Parameter %s is not annotated with @Column", name));
	}

	@Nullable
	@Override
	public Integer getOrdinal() {
		throw new IllegalStateException(String.format("Parameter %s is not annotated with @Element", name));
	}

	@Nullable
	@Override
	public Ordering getPrimaryKeyOrdering() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isClusterKeyColumn() {
		return false;
	}

	@Override
	public boolean isCompositePrimaryKey() {
		return false;
	}

	@Override
	public boolean isMapLike() {
		return typeInformation.isMap();
	}

	@Override
	public boolean isPartitionKeyColumn() {
		return false;
	}

	@Override
	public boolean isPrimaryKeyColumn() {
		return false;
	}

	@Override
	public boolean isStaticColumn() {
		return false;
	}

	@Nullable
	@Override
	public AnnotatedType findAnnotatedType(Class<? extends Annotation> annotationType) {
		return null;
	}

	@Override
	public PersistentEntity<?, CassandraPersistentProperty> getOwner() {
		return owner;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Class<?> getType() {
		return null;
	}

	@Override
	public TypeInformation<?> getTypeInformation() {
		return typeInformation;
	}

	@Override
	public Iterable<? extends TypeInformation<?>> getPersistentEntityTypes() {
		return Collections.emptyList();
	}

	@Nullable
	@Override
	public Method getGetter() {
		return null;
	}

	@Nullable
	@Override
	public Method getSetter() {
		return null;
	}

	@Nullable
	@Override
	public Method getWither() {
		return null;
	}

	@Nullable
	@Override
	public Field getField() {
		return null;
	}

	@Nullable
	@Override
	public String getSpelExpression() {
		return null;
	}

	@Nullable
	@Override
	public Association<CassandraPersistentProperty> getAssociation() {
		return null;
	}

	@Override
	public boolean isEntity() {
		return false;
	}

	@Override
	public boolean isIdProperty() {
		return false;
	}

	@Override
	public boolean isVersionProperty() {
		return false;
	}

	@Override
	public boolean isCollectionLike() {
		return typeInformation.isCollectionLike();
	}

	@Override
	public boolean isMap() {
		return typeInformation.isMap();
	}

	@Override
	public boolean isArray() {
		return typeInformation.getType().isArray();
	}

	@Override
	public boolean isTransient() {
		return false;
	}

	@Override
	public boolean isWritable() {
		return false;
	}

	@Override
	public boolean isImmutable() {
		return false;
	}

	@Override
	public boolean isAssociation() {
		return false;
	}

	@Nullable
	@Override
	public Class<?> getComponentType() {
		return Optional.ofNullable(typeInformation.getComponentType()).map(TypeInformation::getType).orElse(null);
	}

	@Override
	public Class<?> getRawType() {
		return typeInformation.getType();
	}

	@Nullable
	@Override
	public Class<?> getMapValueType() {
		return Optional.ofNullable(typeInformation.getMapValueType()).map(TypeInformation::getType).orElse(null);
	}

	@Override
	public Class<?> getActualType() {
		return typeInformation.getRequiredActualType().getType();
	}

	@Nullable
	@Override
	public <A extends Annotation> A findAnnotation(Class<A> annotationType) {
		return null;
	}

	@Nullable
	@Override
	public <A extends Annotation> A findPropertyOrOwnerAnnotation(Class<A> annotationType) {
		return null;
	}

	@Override
	public boolean isAnnotationPresent(Class<? extends Annotation> annotationType) {
		return false;
	}

	@Override
	public boolean usePropertyAccess() {
		return false;
	}

	@Nullable
	@Override
	public Class<?> getAssociationTargetType() {
		return null;
	}

	@Override
	public void setColumnName(CqlIdentifier columnName) {
		throw new UnsupportedOperationException();
	}

	@Override
	@Deprecated
	public void setForceQuote(boolean forceQuote) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		throw new UnsupportedOperationException();
	}
}
