/*
 * Copyright 2013-2021 the original author or authors.
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
import java.util.function.Supplier;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.expression.BeanFactoryAccessor;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.util.SpelUtils;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.model.AnnotationBasedPersistentProperty;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.Optionals;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Cassandra specific {@link org.springframework.data.mapping.model.AnnotationBasedPersistentProperty} implementation.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Antoine Toulme
 * @author Mark Paluch
 * @author John Blum
 */
public class BasicCassandraPersistentProperty extends AnnotationBasedPersistentProperty<CassandraPersistentProperty>
		implements CassandraPersistentProperty, ApplicationContextAware {

	// Indicates whether this property has been explicitly instructed to force quoted column names.
	private Boolean forceQuote;

	private @Nullable CqlIdentifier columnName;

	private NamingStrategy namingStrategy = NamingStrategy.INSTANCE;

	private @Nullable StandardEvaluationContext spelContext;

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

	/* (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	@Override
	public void setApplicationContext(ApplicationContext context) {

		Assert.notNull(context, "ApplicationContext must not be null");

		this.spelContext = new StandardEvaluationContext();
		this.spelContext.addPropertyAccessor(new BeanFactoryAccessor());
		this.spelContext.setBeanResolver(new BeanFactoryResolver(context));
		this.spelContext.setRootObject(context);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mapping.model.AbstractPersistentProperty#getOwner()
	 */
	@Override
	public CassandraPersistentEntity<?> getOwner() {
		return (CassandraPersistentEntity<?>) super.getOwner();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty#getColumnName()
	 */
	@Override
	public CqlIdentifier getColumnName() {

		if (this.columnName == null) {
			this.columnName = determineColumnName();
		}

		Assert.state(this.columnName != null, () -> String.format("Cannot determine column name for %s", this));

		return this.columnName;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty#getOrdinal()
	 */
	@Nullable
	@Override
	public Integer getOrdinal() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty#getPrimaryKeyOrdering()
	 */
	@Nullable
	@Override
	public Ordering getPrimaryKeyOrdering() {

		PrimaryKeyColumn annotation = findAnnotation(PrimaryKeyColumn.class);

		return annotation != null ? annotation.ordering() : null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty#isCompositePrimaryKey()
	 */
	@Override
	public boolean isCompositePrimaryKey() {
		return AnnotatedElementUtils.findMergedAnnotation(getType(), PrimaryKeyClass.class) != null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty#isClusterKeyColumn()
	 */
	@Override
	public boolean isClusterKeyColumn() {

		PrimaryKeyColumn annotation = findAnnotation(PrimaryKeyColumn.class);

		return annotation != null && PrimaryKeyType.CLUSTERED.equals(annotation.type());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty#isPartitionKeyColumn()
	 */
	@Override
	public boolean isPartitionKeyColumn() {

		PrimaryKeyColumn annotation = findAnnotation(PrimaryKeyColumn.class);

		return annotation != null && PrimaryKeyType.PARTITIONED.equals(annotation.type());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty#isPrimaryKeyColumn()
	 */
	@Override
	public boolean isPrimaryKeyColumn() {
		return isAnnotationPresent(PrimaryKeyColumn.class);
	}

	@Nullable
	private CqlIdentifier determineColumnName() {

		if (isCompositePrimaryKey()) { // then the id type has @PrimaryKeyClass
			return null;
		}

		Supplier<String> defaultName = () -> getNamingStrategy().getColumnName(this);

		String overriddenName = null;

		boolean forceQuote = false;

		if (isIdProperty()) { // then the id is of a simple type (since it's not a composite primary key)

			PrimaryKey primaryKey = findAnnotation(PrimaryKey.class);

			if (primaryKey != null) {
				overriddenName = primaryKey.value();
				forceQuote = primaryKey.forceQuote();
			}

		} else if (isPrimaryKeyColumn()) { // then it's a simple type

			PrimaryKeyColumn primaryKeyColumn = findAnnotation(PrimaryKeyColumn.class);

			if (primaryKeyColumn != null) {
				overriddenName = primaryKeyColumn.value();
				forceQuote = primaryKeyColumn.forceQuote();
			}

		} else { // then it's a vanilla column with the assumption that it's mapped to a single column

			Column column = findAnnotation(Column.class);

			if (column != null) {
				overriddenName = column.value();
				forceQuote = column.forceQuote();
			}
		}

		return createColumnName(defaultName, overriddenName, forceQuote);
	}

	@Nullable
	private CqlIdentifier createColumnName(Supplier<String> defaultName, @Nullable String overriddenName,
			boolean forceQuote) {

		String name;

		if (StringUtils.hasText(overriddenName)) {
			name = this.spelContext != null ? SpelUtils.evaluate(overriddenName, this.spelContext) : overriddenName;
		} else {
			name = defaultName.get();
		}

		return name != null ? IdentifierFactory.create(name, forceQuote) : null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty#setColumnName(org.springframework.data.cassandra.core.cql.CqlIdentifier)
	 */
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

		Assert.notNull(namingStrategy, "NamingStrategy must not be null");

		this.namingStrategy = namingStrategy;
	}

	NamingStrategy getNamingStrategy() {
		return this.namingStrategy;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty#setForceQuote(boolean)
	 */
	@Override
	public void setForceQuote(boolean forceQuote) {

		boolean changed = !Boolean.valueOf(forceQuote).equals(this.forceQuote);

		this.forceQuote = forceQuote;

		if (changed) {
			setColumnName(IdentifierFactory.create(getRequiredColumnName().asInternal(), forceQuote));
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mapping.model.AbstractPersistentProperty#getAssociation()
	 */
	@Override
	public Association<CassandraPersistentProperty> getAssociation() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mapping.model.AbstractPersistentProperty#createAssociation()
	 */
	@Override
	protected Association<CassandraPersistentProperty> createAssociation() {
		return new Association<>(this, this);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty#isMapLike()
	 */
	@Override
	public boolean isMapLike() {
		return ClassUtils.isAssignable(Map.class, getType());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty#findAnnotatedType(java.lang.Class)
	 */
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
