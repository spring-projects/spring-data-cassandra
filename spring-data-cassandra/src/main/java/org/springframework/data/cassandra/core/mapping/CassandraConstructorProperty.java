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
import java.util.Collections;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.core.TypeInformation;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.Parameter;
import org.springframework.data.mapping.PersistentEntity;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Annotated {@link CassandraPersistentProperty} synthesized from a constructor parameter.
 *
 * @author Mark Paluch
 * @since 3.2
 */
class CassandraConstructorProperty implements CassandraPersistentProperty {

	private final Parameter<?, CassandraPersistentProperty> constructorParameter;

	private final String name;

	private final CassandraPersistentEntity<?> owner;

	private final TypeInformation<?> typeInformation;

	public CassandraConstructorProperty(Parameter<?, CassandraPersistentProperty> constructorParameter,
			CassandraPersistentEntity<?> owner) {
		this.constructorParameter = constructorParameter;

		String name = constructorParameter.getName();
		if (name == null) {
			throw new IllegalArgumentException(
					String.format("Constructor parameter %s must have a name", constructorParameter));
		}

		this.name = name;
		this.owner = owner;
		this.typeInformation = constructorParameter.getType();
	}

	@Override
	public CqlIdentifier getColumnName() {
		throw new IllegalStateException(String.format("Parameter %s is not annotated with @Column", name));
	}

	@Override
	public @Nullable Integer getOrdinal() {
		throw new IllegalStateException(String.format("Parameter %s is not annotated with @Element", name));
	}

	@Override
	public @Nullable Ordering getPrimaryKeyOrdering() {
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

	@Override
	public @Nullable AnnotatedType findAnnotatedType(Class<? extends Annotation> annotationType) {
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
	public boolean hasExplicitColumnName() {
		return false;
	}

	@Override
	public Class<?> getType() {
		return constructorParameter.getRawType();
	}

	@Override
	public TypeInformation<?> getTypeInformation() {
		return typeInformation;
	}

	@Override
	public @Nullable Method getGetter() {
		return null;
	}

	@Override
	public @Nullable Method getSetter() {
		return null;
	}

	@Override
	public @Nullable Method getWither() {
		return null;
	}

	@Override
	public @Nullable Field getField() {
		return null;
	}

	@Override
	public @Nullable String getSpelExpression() {
		return null;
	}

	@Override
	public @Nullable Association<CassandraPersistentProperty> getAssociation() {
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
	public boolean isReadable() {
		return true;
	}

	@Override
	public boolean isImmutable() {
		return false;
	}

	@Override
	public boolean isAssociation() {
		return false;
	}

	@Override
	public @Nullable Class<?> getComponentType() {
		return Optional.ofNullable(typeInformation.getComponentType()).map(TypeInformation::getType).orElse(null);
	}

	@Override
	public Class<?> getRawType() {
		return typeInformation.getType();
	}

	@Override
	public @Nullable Class<?> getMapValueType() {
		return Optional.ofNullable(typeInformation.getMapValueType()).map(TypeInformation::getType).orElse(null);
	}

	@Override
	public Class<?> getActualType() {
		return typeInformation.getRequiredActualType().getType();
	}

	@Override
	public <A extends Annotation> @Nullable A findAnnotation(Class<A> annotationType) {
		return null;
	}

	@Override
	public <A extends Annotation> @Nullable A findPropertyOrOwnerAnnotation(Class<A> annotationType) {
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

	@Override
	public @Nullable Class<?> getAssociationTargetType() {
		return null;
	}

	@Override
	public Iterable<? extends TypeInformation<?>> getPersistentEntityTypeInformation() {
		return Collections.emptyList();
	}

	@Override
	public @Nullable TypeInformation<?> getAssociationTargetTypeInformation() {
		return null;
	}

	@Override
	public void setColumnName(CqlIdentifier columnName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof CassandraConstructorProperty that)) {
			return false;
		}

		return constructorParameter.equals(that.constructorParameter);
	}

	@Override
	public int hashCode() {
		return constructorParameter.hashCode();
	}
}
