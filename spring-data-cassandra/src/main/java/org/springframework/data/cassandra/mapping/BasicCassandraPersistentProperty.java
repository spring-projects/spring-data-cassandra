/*
 * Copyright 2013-2017 the original author or authors
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

import static org.springframework.cassandra.core.cql.CqlIdentifier.*;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.cassandra.core.Ordering;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.expression.BeanFactoryAccessor;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.util.SpelUtils;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.AnnotationBasedPersistentProperty;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.UserType;

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

	protected ApplicationContext context;

	/**
	 * Whether this property has been explicitly instructed to force quote column names.
	 */
	protected Boolean forceQuote;

	/**
	 * An unmodifiable list of this property's column names.
	 */
	protected List<CqlIdentifier> columnNames;

	/**
	 * An unmodifiable list of this property's explicitly set column names.
	 */
	protected List<CqlIdentifier> explicitColumnNames;

	protected StandardEvaluationContext spelContext;

	private final UserTypeResolver userTypeResolver;

	/**
	 * Creates a new {@link BasicCassandraPersistentProperty}.
	 *
	 * @param field the actual {@link Field} in the domain entity corresponding to this persistent entity.
	 * @param propertyDescriptor a {@link PropertyDescriptor} for the corresponding property in the domain entity.
	 * @param owner the containing object or {@link CassandraPersistentEntity} of this persistent property.
	 * @param simpleTypeHolder mapping of Java [simple|wrapper] types to Cassandra data types.
	 */
	public BasicCassandraPersistentProperty(Field field, PropertyDescriptor propertyDescriptor,
			CassandraPersistentEntity<?> owner, CassandraSimpleTypeHolder simpleTypeHolder) {

		this(field, propertyDescriptor, owner, simpleTypeHolder, null);
	}

	/**
	 * Creates a new {@link BasicCassandraPersistentProperty}.
	 *
	 * @param field the actual {@link Field} in the domain entity corresponding to this persistent entity.
	 * @param propertyDescriptor a {@link PropertyDescriptor} for the corresponding property in the domain entity.
	 * @param owner the containing object or {@link CassandraPersistentEntity} of this persistent property.
	 * @param simpleTypeHolder mapping of Java [simple|wrapper] types to Cassandra data types.
	 * @param userTypeResolver resolver for user-defined types.
	 */
	public BasicCassandraPersistentProperty(Field field, PropertyDescriptor propertyDescriptor,
			CassandraPersistentEntity<?> owner, CassandraSimpleTypeHolder simpleTypeHolder,
			UserTypeResolver userTypeResolver) {

		super(field, propertyDescriptor, owner, simpleTypeHolder);

		this.userTypeResolver = userTypeResolver;

		if (owner.getApplicationContext() != null) {
			setApplicationContext(owner.getApplicationContext());
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext context) {

		Assert.notNull(context, "ApplicationContext must not be null");

		this.context = context;
		spelContext = new StandardEvaluationContext();
		spelContext.addPropertyAccessor(new BeanFactoryAccessor());
		spelContext.setBeanResolver(new BeanFactoryResolver(context));
		spelContext.setRootObject(context);
	}

	@Override
	public CassandraPersistentEntity<?> getOwner() {
		return (CassandraPersistentEntity<?>) super.getOwner();
	}

	@Override
	public boolean isCompositePrimaryKey() {
		return (AnnotatedElementUtils.findMergedAnnotation(getType(), PrimaryKeyClass.class) != null);
	}

	public Class<?> getCompositePrimaryKeyType() {
		return (isCompositePrimaryKey() ? getType() : null);
	}

	@Override
	public TypeInformation<?> getCompositePrimaryKeyTypeInformation() {
		return (isCompositePrimaryKey() ? ClassTypeInformation.from(getCompositePrimaryKeyType()) : null);
	}

	@Override
	public CqlIdentifier getColumnName() {

		List<CqlIdentifier> columnNames = getColumnNames();

		Assert.state(columnNames.size() == 1, String.format("Property [%s] has no single column mapping", getName()));

		return columnNames.get(0);
	}

	@Override
	public Ordering getPrimaryKeyOrdering() {

		PrimaryKeyColumn primaryKeyColumn = findAnnotation(PrimaryKeyColumn.class);

		return (primaryKeyColumn != null ? primaryKeyColumn.ordering() : null);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.mapping.CassandraPersistentProperty#getDataType()
	 */
	@Override
	public DataType getDataType() {

		DataType dataType = findDataType();

		if (dataType == null) {
			throw new InvalidDataAccessApiUsageException(String.format(
					"Unknown type [%s] for property [%s] in entity [%s]; only primitive types and Collections or Maps of primitive types are allowed",
					getType(), getName(), getOwner().getName()));
		}

		return dataType;
	}

	private DataType findDataType() {

		CassandraType cassandraType = findAnnotation(CassandraType.class);

		if (cassandraType != null) {
			return getDataTypeFor(cassandraType);
		}

		if (isMap()) {
			List<TypeInformation<?>> args = getTypeInformation().getTypeArguments();

			ensureTypeArguments(args.size(), 2);

			return DataType.map(getDataTypeFor(args.get(0).getType()), getDataTypeFor(args.get(1).getType()));
		}

		if (isCollectionLike()) {
			List<TypeInformation<?>> args = getTypeInformation().getTypeArguments();

			ensureTypeArguments(args.size(), 1);

			if (Set.class.isAssignableFrom(getType())) {
				return DataType.set(getDataTypeFor(args.get(0).getType()));
			}

			if (List.class.isAssignableFrom(getType())) {
				return DataType.list(getDataTypeFor(args.get(0).getType()));
			}
		}

		return CassandraSimpleTypeHolder.getDataTypeFor(getType());
	}

	private DataType getDataTypeFor(CassandraType annotation) {

		DataType.Name type = annotation.type();

		switch (type) {
			case MAP:
				ensureTypeArguments(annotation.typeArguments().length, 2);
				return DataType.map(getDataTypeFor(annotation.typeArguments()[0]),
						getDataTypeFor(annotation.typeArguments()[1]));
			case LIST:
				ensureTypeArguments(annotation.typeArguments().length, 1);
				if (annotation.typeArguments()[0] == Name.UDT) {
					return DataType.list(getUserType(annotation));
				}
				return DataType.list(getDataTypeFor(annotation.typeArguments()[0]));
			case SET:
				ensureTypeArguments(annotation.typeArguments().length, 1);
				if (annotation.typeArguments()[0] == Name.UDT) {
					return DataType.set(getUserType(annotation));
				}
				return DataType.set(getDataTypeFor(annotation.typeArguments()[0]));
			case UDT:
				return getUserType(annotation);
			default:
				return CassandraSimpleTypeHolder.getDataTypeFor(type);
		}
	}

	private DataType getUserType(CassandraType annotation) {

		if (!StringUtils.hasText(annotation.userTypeName())) {
			throw new InvalidDataAccessApiUsageException(
					String.format("Expected user type name in property ['%s'] of type ['%s'] in entity [%s]", getName(),
							getType(), getOwner().getName()));
		}

		CqlIdentifier identifier = CqlIdentifier.cqlId(annotation.userTypeName());
		UserType userType = userTypeResolver.resolveType(identifier);

		if (userType == null) {
			throw new MappingException(String.format("User type [%s] not found", identifier));
		}

		return userType;
	}

	@Override
	public boolean isIndexed() {
		return isAnnotationPresent(Indexed.class);
	}

	@Override
	public boolean isClusterKeyColumn() {

		PrimaryKeyColumn primaryKeyColumn = findAnnotation(PrimaryKeyColumn.class);

		return (primaryKeyColumn != null && PrimaryKeyType.CLUSTERED.equals(primaryKeyColumn.type()));
	}

	@Override
	public boolean isPartitionKeyColumn() {

		PrimaryKeyColumn primaryKeyColumn = findAnnotation(PrimaryKeyColumn.class);

		return (primaryKeyColumn != null && PrimaryKeyType.PARTITIONED.equals(primaryKeyColumn.type()));
	}

	@Override
	public boolean isPrimaryKeyColumn() {
		return isAnnotationPresent(PrimaryKeyColumn.class);
	}

	protected DataType getDataTypeFor(DataType.Name dataTypeName) {

		DataType dataType = CassandraSimpleTypeHolder.getDataTypeFor(dataTypeName);

		if (dataType == null) {
			throw new InvalidDataAccessApiUsageException(String.format(
					"Only primitive types are allowed inside Collections for property [%1$s] of type [%2$s] in entity [%3$s]",
					getName(), getType(), getOwner().getName()));
		}

		return dataType;
	}

	protected DataType getDataTypeFor(Class<?> javaType) {

		CassandraPersistentEntity<?> persistentEntity = getOwner().getMappingContext().getPersistentEntity(javaType);

		if (persistentEntity != null && persistentEntity.isUserDefinedType()) {
			return persistentEntity.getUserType();
		}

		DataType dataType = CassandraSimpleTypeHolder.getDataTypeFor(javaType);

		if (dataType == null) {
			throw new InvalidDataAccessApiUsageException(String.format(
					"Only primitive types are allowed inside Collections for property [%1$s] of type ['%2$s'] in entity [%3$s]",
					getName(), getType(), getOwner().getName()));
		}

		return dataType;
	}

	protected void ensureTypeArguments(int args, int expected) {
		if (args != expected) {
			throw new InvalidDataAccessApiUsageException(
					String.format("Expected [%1$s] typed arguments for property ['%2$s'] of type ['%3$s'] in entity [%4$s]",
							expected, getName(), getType(), getOwner().getName()));
		}
	}

	@Override
	public List<CqlIdentifier> getColumnNames() {

		columnNames = (columnNames != null ? columnNames : Collections.unmodifiableList(determineColumnNames()));

		return columnNames;
	}

	protected List<CqlIdentifier> determineColumnNames() {

		List<CqlIdentifier> columnNames = new ArrayList<CqlIdentifier>();

		if (isCompositePrimaryKey()) { // then the id type has @PrimaryKeyClass
			addCompositePrimaryKeyColumnNames(getCompositePrimaryKeyEntity(), columnNames);
		} else { // else we're dealing with a single-column field
			String defaultName = getName(); // TODO: replace with naming strategy class
			String overriddenName;
			boolean forceQuote;

			if (isIdProperty()) { // then the id is of a simple type (since it's not a composite primary key)
				PrimaryKey primaryKey = findAnnotation(PrimaryKey.class);
				overriddenName = primaryKey == null ? null : primaryKey.value();
				forceQuote = (primaryKey != null && primaryKey.forceQuote());

			} else if (isPrimaryKeyColumn()) { // then it's a simple type
				PrimaryKeyColumn primaryKeyColumn = findAnnotation(PrimaryKeyColumn.class);
				overriddenName = primaryKeyColumn == null ? null : primaryKeyColumn.name();
				forceQuote = (primaryKeyColumn != null && primaryKeyColumn.forceQuote());

			} else { // then it's a vanilla column with the assumption that it's mapped to a single column
				Column column = findAnnotation(Column.class);
				overriddenName = column == null ? null : column.value();
				forceQuote = (column != null && column.forceQuote());
			}

			columnNames.add(createColumnName(defaultName, overriddenName, forceQuote));

		}

		return columnNames;
	}

	protected CqlIdentifier createColumnName(String defaultName, String overriddenName, boolean forceQuote) {

		String name = defaultName;

		if (StringUtils.hasText(overriddenName)) {
			name = (spelContext != null ? SpelUtils.evaluate(overriddenName, spelContext) : overriddenName);
		}

		return cqlId(name, forceQuote);
	}

	protected void addCompositePrimaryKeyColumnNames(CassandraPersistentEntity<?> compositePrimaryKeyEntity,
			final List<CqlIdentifier> columnNames) {

		compositePrimaryKeyEntity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(CassandraPersistentProperty property) {
				if (property.isCompositePrimaryKey()) {
					addCompositePrimaryKeyColumnNames(property.getCompositePrimaryKeyEntity(), columnNames);
				} else {
					columnNames.add(property.getColumnName());
				}
			}
		});
	}

	@Override
	public void setColumnName(CqlIdentifier columnName) {

		Assert.notNull(columnName, "columnName must not be null");

		setColumnNames(Collections.singletonList(columnName));
	}

	@Override
	public void setColumnNames(List<CqlIdentifier> columnNames) {

		Assert.notNull(columnNames, "List of column names must not be null");

		// force calculation of columnNames if not known yet
		getColumnNames();

		Assert.state(this.columnNames.size() == columnNames.size(),
				String.format(
						"Property [%s] of entity [%s] is mapped to [%s] column%s, but given column name list has size [%s]",
						getName(), getOwner().getType().getName(), this.columnNames.size(), this.columnNames.size() == 1 ? "" : "s",
						columnNames.size()));

		this.columnNames = this.explicitColumnNames = Collections
				.unmodifiableList(new ArrayList<CqlIdentifier>(columnNames));
	}

	@Override
	public void setForceQuote(boolean forceQuote) {

		if (this.forceQuote != null && this.forceQuote == forceQuote) {
			return;
		} else {
			this.forceQuote = forceQuote;
		}

		List<CqlIdentifier> columnNames = new ArrayList<CqlIdentifier>(
				this.columnNames == null ? 0 : this.columnNames.size());

		for (CqlIdentifier columnName : getColumnNames()) {
			columnNames.add(cqlId(columnName.getUnquoted(), forceQuote));
		}

		setColumnNames(columnNames);
	}

	@Override
	public List<CassandraPersistentProperty> getCompositePrimaryKeyProperties() {

		Assert.state(isCompositePrimaryKey(),
				String.format("[%s] does not represent a composite primary key property", getName()));

		return getCompositePrimaryKeyEntity().getCompositePrimaryKeyProperties();
	}

	@Override
	public CassandraPersistentEntity<?> getCompositePrimaryKeyEntity() {

		CassandraMappingContext mappingContext = getOwner().getMappingContext();

		Assert.state(mappingContext != null, "CassandraMappingContext needed");

		return mappingContext.getPersistentEntity(getCompositePrimaryKeyTypeInformation());
	}

	@Override
	public Association<CassandraPersistentProperty> getAssociation() {
		throw new UnsupportedOperationException("Cassandra does not support associations");
	}

	@Override
	protected Association<CassandraPersistentProperty> createAssociation() {
		return new Association<CassandraPersistentProperty>(this, null);
	}

	@Override
	public boolean isMapLike() {
		return ClassUtils.isAssignable(Map.class, getType());
	}
}
