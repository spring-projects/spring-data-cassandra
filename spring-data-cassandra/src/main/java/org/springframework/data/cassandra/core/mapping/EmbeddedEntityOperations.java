/*
 * Copyright 2020-2025 the original author or authors.
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

import org.jspecify.annotations.Nullable;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.mapping.*;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.StringUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Support methods to obtain {@link PersistentProperty} and {@link PersistentEntity} for embedded properties.
 *
 * @author Christoph Strobl
 * @since 3.0
 * @see Embedded
 */
public class EmbeddedEntityOperations {

	private final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

	public EmbeddedEntityOperations(
			MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext) {
		this.mappingContext = mappingContext;
	}

	public CassandraPersistentEntity<?> getEntity(CassandraPersistentProperty property) {
		return withPrefix(getPrefix(property), mappingContext.getRequiredPersistentEntity(property));
	}

	static <T> CassandraPersistentEntity<T> withPrefix(@Nullable String prefix,
			CassandraPersistentEntity<T> source) {

		if (!StringUtils.hasText(prefix)) {
			return source;
		}

		return new PrefixedCassandraPersistentEntity<>(prefix, source);
	}

	@Nullable
	static String getPrefix(CassandraPersistentProperty property) {

		Embedded embedded = property.findAnnotation(Embedded.class);
		return embedded != null ? embedded.prefix() : null;
	}

	static class PrefixedCassandraPersistentEntity<T> implements CassandraPersistentEntity<T> {

		private final String prefix;
		private final CassandraPersistentEntity<T> delegate;

		public PrefixedCassandraPersistentEntity(String prefix, CassandraPersistentEntity<T> delegate) {

			this.prefix = prefix;
			this.delegate = delegate;
		}

		@Override
		public boolean isCompositePrimaryKey() {
			return delegate.isCompositePrimaryKey();
		}

		@Override
		public CqlIdentifier getTableName() {
			return delegate.getTableName();
		}

		@Override
		public void setTableName(CqlIdentifier tableName) {
			delegate.setTableName(tableName);
		}

		@Override
		@Nullable
		public CqlIdentifier getKeyspace() {
			return delegate.getKeyspace();
		}

		@Override
		public void setKeyspace(CqlIdentifier keyspace) {
			delegate.setKeyspace(keyspace);
		}

		@Override
		public boolean isTupleType() {
			return delegate.isTupleType();
		}

		@Override
		public boolean isUserDefinedType() {
			return delegate.isUserDefinedType();
		}

		@Override
		public String getName() {
			return delegate.getName();
		}

		@Override
		@Nullable
		public InstanceCreatorMetadata<CassandraPersistentProperty> getInstanceCreatorMetadata() {
			return delegate.getInstanceCreatorMetadata();
		}

		@Override
		public boolean isCreatorArgument(PersistentProperty<?> property) {
			return delegate.isCreatorArgument(property);
		}

		@Override
		public boolean isIdProperty(PersistentProperty<?> property) {
			return delegate.isIdProperty(property);
		}

		@Override
		public boolean isVersionProperty(PersistentProperty<?> property) {
			return delegate.isVersionProperty(property);
		}

		@Override
		@Nullable
		public CassandraPersistentProperty getIdProperty() {
			return delegate.getIdProperty();
		}

		@Override
		public CassandraPersistentProperty getRequiredIdProperty() {
			return delegate.getRequiredIdProperty();
		}

		@Override
		@Nullable
		public CassandraPersistentProperty getVersionProperty() {
			return delegate.getVersionProperty();
		}

		@Override
		public CassandraPersistentProperty getRequiredVersionProperty() {
			return delegate.getRequiredVersionProperty();
		}

		@Override
		@Nullable
		public CassandraPersistentProperty getPersistentProperty(String name) {

			CassandraPersistentProperty property = delegate.getPersistentProperty(name);

			return property == null ? null : new PrefixedCassandraPersistentProperty(prefix, property);
		}

		@Override
		public CassandraPersistentProperty getRequiredPersistentProperty(String name) {
			return new PrefixedCassandraPersistentProperty(prefix, delegate.getRequiredPersistentProperty(name));
		}

		@Override
		@Nullable
		public CassandraPersistentProperty getPersistentProperty(Class<? extends Annotation> annotationType) {

			CassandraPersistentProperty property = delegate.getPersistentProperty(annotationType);

			return property == null ? null : new PrefixedCassandraPersistentProperty(prefix, property);
		}

		@Override
		public Iterable<CassandraPersistentProperty> getPersistentProperties(Class<? extends Annotation> annotationType) {
			return delegate.getPersistentProperties(annotationType);
		}

		@Override
		public boolean hasIdProperty() {
			return delegate.hasIdProperty();
		}

		@Override
		public boolean hasVersionProperty() {
			return delegate.hasVersionProperty();
		}

		@Override
		public Class<T> getType() {
			return delegate.getType();
		}

		@Override
		public Alias getTypeAlias() {
			return delegate.getTypeAlias();
		}

		@Override
		public TypeInformation<T> getTypeInformation() {
			return delegate.getTypeInformation();
		}

		@Override
		public void doWithProperties(PropertyHandler<CassandraPersistentProperty> handler) {

			delegate.doWithProperties((PropertyHandler<CassandraPersistentProperty>) property -> {
				handler.doWithPersistentProperty(wrap(property));
			});
		}

		@Override
		public void doWithProperties(SimplePropertyHandler handler) {

			delegate.doWithProperties((SimplePropertyHandler) property -> {

				if (property instanceof CassandraPersistentProperty) {
					handler.doWithPersistentProperty(wrap((CassandraPersistentProperty) property));
				} else {
					handler.doWithPersistentProperty(property);
				}
			});
			delegate.doWithProperties(handler);
		}

		@Override
		public void doWithAssociations(AssociationHandler<CassandraPersistentProperty> handler) {
			delegate.doWithAssociations(handler);
		}

		@Override
		public void doWithAssociations(SimpleAssociationHandler handler) {
			delegate.doWithAssociations(handler);
		}

		@Override
		@Nullable
		public <A extends Annotation> A findAnnotation(Class<A> annotationType) {
			return delegate.findAnnotation(annotationType);
		}

		@Override
		public <A extends Annotation> A getRequiredAnnotation(Class<A> annotationType) throws IllegalStateException {
			return delegate.getRequiredAnnotation(annotationType);
		}

		@Override
		public <A extends Annotation> boolean isAnnotationPresent(Class<A> annotationType) {
			return delegate.isAnnotationPresent(annotationType);
		}

		@Override
		public <B> PersistentPropertyAccessor<B> getPropertyAccessor(B bean) {
			return delegate.getPropertyAccessor(bean);
		}

		@Override
		public <B> PersistentPropertyPathAccessor<B> getPropertyPathAccessor(B bean) {
			return delegate.getPropertyPathAccessor(bean);
		}

		@Override
		public IdentifierAccessor getIdentifierAccessor(Object bean) {
			return delegate.getIdentifierAccessor(bean);
		}

		@Override
		public boolean isNew(Object bean) {
			return delegate.isNew(bean);
		}

		@Override
		public boolean isImmutable() {
			return delegate.isImmutable();
		}

		@Override
		public boolean requiresPropertyPopulation() {
			return delegate.requiresPropertyPopulation();
		}

		@Nullable
		@Override
		public CassandraPersistentProperty getProperty(Parameter<?, CassandraPersistentProperty> parameter) {
			CassandraPersistentProperty property = delegate.getProperty(parameter);
			return property == null ? null : wrap(property);
		}

		@Override
		public Iterator<CassandraPersistentProperty> iterator() {

			List<CassandraPersistentProperty> target = new ArrayList<>();
			delegate.iterator().forEachRemaining(it -> target.add(wrap(it)));
			return target.iterator();
		}

		@Override
		public void forEach(Consumer<? super CassandraPersistentProperty> action) {
			delegate.forEach(it -> action.accept(wrap(it)));
		}

		@Override
		public Spliterator<CassandraPersistentProperty> spliterator() {
			return delegate.spliterator();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (!(o instanceof PrefixedCassandraPersistentEntity<?> that)) {
				return false;
			}

			if (!prefix.equals(that.prefix)) {
				return false;
			}
			return delegate.equals(that.delegate);
		}

		@Override
		public int hashCode() {
			int result = prefix.hashCode();
			result = 31 * result + delegate.hashCode();
			return result;
		}

		private PrefixedCassandraPersistentProperty wrap(CassandraPersistentProperty source) {
			return new PrefixedCassandraPersistentProperty(prefix, source);
		}
	}

	static class PrefixedCassandraPersistentProperty implements CassandraPersistentProperty {

		private final String prefix;
		private final CassandraPersistentProperty delegate;

		public PrefixedCassandraPersistentProperty(String prefix, CassandraPersistentProperty delegate) {
			this.prefix = prefix;
			this.delegate = delegate;
		}

		@Override
		public void setColumnName(CqlIdentifier columnName) {
			delegate.setColumnName(columnName);
		}

		@Override
		public CqlIdentifier getColumnName() {
			return CqlIdentifier.fromInternal(prefix + delegate.getRequiredColumnName().asInternal());
		}

		@Override
		public boolean hasExplicitColumnName() {
			return false;
		}

		@Override
		@Nullable
		public Integer getOrdinal() {
			return delegate.getOrdinal();
		}

		@Override
		public int getRequiredOrdinal() {
			return delegate.getRequiredOrdinal();
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
		public boolean isEmbedded() {
			return delegate.isEmbedded();
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
		public Method getRequiredGetter() {
			return delegate.getRequiredGetter();
		}

		@Override
		@Nullable
		public Method getSetter() {
			return delegate.getSetter();
		}

		@Override
		public Method getRequiredSetter() {
			return delegate.getRequiredSetter();
		}

		@Override
		@Nullable
		public Method getWither() {
			return delegate.getWither();
		}

		@Override
		public Method getRequiredWither() {
			return delegate.getRequiredWither();
		}

		@Override
		@Nullable
		public Field getField() {
			return delegate.getField();
		}

		@Override
		public Field getRequiredField() {
			return delegate.getRequiredField();
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
		public Association<CassandraPersistentProperty> getRequiredAssociation() {
			return delegate.getRequiredAssociation();
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
		public <A extends Annotation> A getRequiredAnnotation(Class<A> annotationType) throws IllegalStateException {
			return delegate.getRequiredAnnotation(annotationType);
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
		public boolean hasActualTypeAnnotation(Class<? extends Annotation> annotationType) {
			return delegate.hasActualTypeAnnotation(annotationType);
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
		public <T> PersistentPropertyAccessor<T> getAccessorForOwner(T owner) {
			return delegate.getAccessorForOwner(owner);
		}

		@Override
		public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
			delegate.setApplicationContext(applicationContext);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (!(o instanceof PrefixedCassandraPersistentProperty that)) {
				return false;
			}

			if (!prefix.equals(that.prefix)) {
				return false;
			}
			return delegate.equals(that.delegate);
		}

		@Override
		public int hashCode() {
			int result = prefix.hashCode();
			result = 31 * result + delegate.hashCode();
			return result;
		}
	}
}
