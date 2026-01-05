/*
 * Copyright 2013-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedParameterizedType;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.model.AnnotationBasedPersistentProperty;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.Optionals;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Cassandra specific {@link org.springframework.data.mapping.model.AnnotationBasedPersistentProperty} implementation.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Antoine Toulme
 * @author Mark Paluch
 * @author John Blum
 * @author Aleksei Zotov
 */
public class BasicCassandraPersistentProperty extends AnnotationBasedPersistentProperty<CassandraPersistentProperty>
		implements CassandraPersistentProperty, ApplicationContextAware {

	private final CqlIdentifierGenerator namingAccessor = new CqlIdentifierGenerator();

	// Indicates whether this property has been explicitly instructed to force quoted column names.
	private Boolean forceQuote;

	private @Nullable CqlIdentifier columnName;

	/**
	 * Create a new {@link BasicCassandraPersistentProperty}.
	 *
	 * @param property the actual {@link Property} in the domain entity corresponding to this persistent entity.
	 * @param owner the containing object or {@link CassandraPersistentEntity} of this persistent property.
	 * @param simpleTypeHolder mapping of Java [simple|wrapper] types to Cassandra data types.
	 */
	public BasicCassandraPersistentProperty(Property property, CassandraPersistentEntity<?> owner,
			SimpleTypeHolder simpleTypeHolder) {

		super(property, owner, simpleTypeHolder);
	}

	@Override
	public void setApplicationContext(ApplicationContext context) {}

	@Override
	public CassandraPersistentEntity<?> getOwner() {
		return (CassandraPersistentEntity<?>) super.getOwner();
	}

	@Override
	public CqlIdentifier getColumnName() {

		if (this.columnName == null) {
			this.columnName = determineColumnName();
		}

		if (columnName == null) {
			throw new IllegalStateException(String.format("Cannot determine column name for %s", this));
		}

		return this.columnName;
	}

	@Nullable
	@Override
	public Integer getOrdinal() {
		return null;
	}

	@Nullable
	@Override
	public Ordering getPrimaryKeyOrdering() {

		PrimaryKeyColumn annotation = findAnnotation(PrimaryKeyColumn.class);

		return annotation != null ? annotation.ordering() : null;
	}

	@Override
	public boolean isCompositePrimaryKey() {
		return AnnotatedElementUtils.findMergedAnnotation(getType(), PrimaryKeyClass.class) != null;
	}

	@Override
	public boolean isClusterKeyColumn() {

		PrimaryKeyColumn annotation = findAnnotation(PrimaryKeyColumn.class);

		return annotation != null && PrimaryKeyType.CLUSTERED.equals(annotation.type());
	}

	@Override
	public boolean isPartitionKeyColumn() {

		PrimaryKeyColumn annotation = findAnnotation(PrimaryKeyColumn.class);

		return annotation != null && PrimaryKeyType.PARTITIONED.equals(annotation.type());
	}

	@Override
	public boolean isPrimaryKeyColumn() {
		return isAnnotationPresent(PrimaryKeyColumn.class);
	}

	@Override
	public boolean isStaticColumn() {

		Column annotation = findAnnotation(Column.class);

		return annotation != null && annotation.isStatic();
	}

	@Nullable
	private CqlIdentifier determineColumnName() {

		if (isCompositePrimaryKey()) { // then the id type has @PrimaryKeyClass
			return null;
		}

		String overriddenName = null;
		boolean forceQuote = false;

		Column column = findAnnotation(Column.class);

		if (column != null) {
			overriddenName = column.value();
			forceQuote = column.forceQuote();
		}

		BasicCassandraPersistentEntity<?> entity = (BasicCassandraPersistentEntity<?>) getOwner();

		return namingAccessor.generate(overriddenName, forceQuote, NamingStrategy::getColumnName, this,
				BasicCassandraPersistentEntity.PARSER, entity::getValueEvaluationContext).getRequiredIdentifier();
	}

	@Override
	public boolean hasExplicitColumnName() {

		if (isCompositePrimaryKey()) {
			return false;
		}

		Column column = findAnnotation(Column.class);
		return column != null && !ObjectUtils.isEmpty(column.value());
	}

	@Override
	public void setColumnName(CqlIdentifier columnName) {

		Assert.notNull(columnName, "ColumnName must not be null");

		this.columnName = columnName;
	}

	/**
	 * Set the {@link NamingStrategy} to use.
	 *
	 * @param namingStrategy must not be {@literal null}.
	 * @since 3.0
	 */
	public void setNamingStrategy(NamingStrategy namingStrategy) {
		this.namingAccessor.setNamingStrategy(namingStrategy);
	}

	@Override
	public void setForceQuote(boolean forceQuote) {

		boolean changed = !Boolean.valueOf(forceQuote).equals(this.forceQuote);

		this.forceQuote = forceQuote;

		if (changed) {
			setColumnName(CqlIdentifierGenerator.createIdentifier(getRequiredColumnName().asInternal(), forceQuote));
		}
	}

	@Override
	public Association<CassandraPersistentProperty> getAssociation() {
		return null;
	}

	@Override
	protected Association<CassandraPersistentProperty> createAssociation() {
		return new Association<>(this, this);
	}

	@Override
	public boolean isMapLike() {
		return ClassUtils.isAssignable(Map.class, getType());
	}

	@Override
	public AnnotatedType findAnnotatedType(Class<? extends Annotation> annotationType) {

		return Optionals
				.toStream(Optional.ofNullable(getField()).map(Field::getAnnotatedType),
						Optional.ofNullable(getGetter()).map(Method::getAnnotatedReturnType),
						Optional.ofNullable(getSetter()).map(it -> it.getParameters()[0].getAnnotatedType()))
				.filter(it -> hasAnnotation(it, annotationType, getTypeInformation())).findFirst().orElse(null);
	}

	private static boolean hasAnnotation(AnnotatedType type, Class<? extends Annotation> annotationType,
			TypeInformation<?> typeInformation) {

		if (AnnotatedElementUtils.hasAnnotation(type, annotationType)) {
			return true;
		}

		if (type instanceof AnnotatedParameterizedType) {

			AnnotatedParameterizedType parameterizedType = (AnnotatedParameterizedType) type;
			AnnotatedType[] arguments = parameterizedType.getAnnotatedActualTypeArguments();

			if (typeInformation.isCollectionLike() && arguments.length == 1) {
				return AnnotatedElementUtils.hasAnnotation(arguments[0], annotationType);
			}

			if (typeInformation.isMap() && arguments.length == 2) {
				return AnnotatedElementUtils.hasAnnotation(arguments[0], annotationType)
						|| AnnotatedElementUtils.hasAnnotation(arguments[1], annotationType);
			}
		}

		return false;
	}
}
