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
import java.util.Set;

import org.springframework.cassandra.core.Ordering;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.model.AnnotationBasedPersistentProperty;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.DataType;

/**
 * Cassandra specific {@link org.springframework.data.mapping.model.AnnotationBasedPersistentProperty} implementation.
 * 
 * @author Alex Shvid
 */
public class BasicCassandraPersistentProperty extends AnnotationBasedPersistentProperty<CassandraPersistentProperty>
		implements CassandraPersistentProperty {

	/**
	 * Creates a new {@link BasicCassandraPersistentProperty}.
	 * 
	 * @param field
	 * @param propertyDescriptor
	 * @param owner
	 * @param simpleTypeHolder
	 */
	public BasicCassandraPersistentProperty(Field field, PropertyDescriptor propertyDescriptor,
			CassandraPersistentEntity<?> owner, SimpleTypeHolder simpleTypeHolder) {
		super(field, propertyDescriptor, owner, simpleTypeHolder);
	}

	@Override
	public boolean isIdProperty() {

		if (super.isIdProperty()) {
			return true;
		}

		return getField().isAnnotationPresent(PrimaryKey.class);
	}

	@Override
	public boolean isCompositePrimaryKey() {
		return getField().getType().isAnnotationPresent(CompositePrimaryKey.class);
	}

	public String getColumnName() {

		// first check @Column annotation
		Column annotation = getField().getAnnotation(Column.class);
		if (annotation != null && StringUtils.hasText(annotation.value())) {
			return annotation.value();
		}

		// else check @KeyColumn annotation
		PrimaryKeyColumn anno = getField().getAnnotation(PrimaryKeyColumn.class);
		if (anno == null || !StringUtils.hasText(anno.value())) {
			return field.getName();
		}
		return anno.value();
	}

	public Ordering getOrdering() {

		PrimaryKeyColumn anno = getField().getAnnotation(PrimaryKeyColumn.class);

		return anno == null ? null : anno.ordering();
	}

	public DataType getDataType() {
		Qualify annotation = getField().getAnnotation(Qualify.class);
		if (annotation != null) {
			return qualifyAnnotatedType(annotation);
		}
		if (isMap()) {
			List<TypeInformation<?>> args = getTypeInformation().getTypeArguments();
			ensureTypeArguments(args.size(), 2);
			return DataType.map(autodetectPrimitiveType(args.get(0).getType()),
					autodetectPrimitiveType(args.get(1).getType()));
		}
		if (isCollectionLike()) {
			List<TypeInformation<?>> args = getTypeInformation().getTypeArguments();
			ensureTypeArguments(args.size(), 1);
			if (Set.class.isAssignableFrom(getType())) {
				return DataType.set(autodetectPrimitiveType(args.get(0).getType()));
			} else if (List.class.isAssignableFrom(getType())) {
				return DataType.list(autodetectPrimitiveType(args.get(0).getType()));
			}
		}
		DataType dataType = CassandraSimpleTypes.autodetectPrimitive(this.getType());
		if (dataType == null) {
			throw new InvalidDataAccessApiUsageException(
					"only primitive types and Set,List,Map collections are allowed, unknown type for property '" + this.getName()
							+ "' type is '" + this.getType() + "' in the entity " + this.getOwner().getName());
		}
		return dataType;
	}

	private DataType qualifyAnnotatedType(Qualify annotation) {
		DataType.Name type = annotation.type();
		if (type.isCollection()) {
			switch (type) {
			case MAP:
				ensureTypeArguments(annotation.typeArguments().length, 2);
				return DataType.map(resolvePrimitiveType(annotation.typeArguments()[0]),
						resolvePrimitiveType(annotation.typeArguments()[1]));
			case LIST:
				ensureTypeArguments(annotation.typeArguments().length, 1);
				return DataType.list(resolvePrimitiveType(annotation.typeArguments()[0]));
			case SET:
				ensureTypeArguments(annotation.typeArguments().length, 1);
				return DataType.set(resolvePrimitiveType(annotation.typeArguments()[0]));
			default:
				throw new InvalidDataAccessApiUsageException("unknown collection DataType for property '" + this.getName()
						+ "' type is '" + this.getType() + "' in the entity " + this.getOwner().getName());
			}
		} else {
			return CassandraSimpleTypes.resolvePrimitive(type);
		}
	}

	public boolean isIndexed() {
		return getField().isAnnotationPresent(Indexed.class);
	}

	public boolean isPartitionKeyColumn() {

		PrimaryKeyColumn anno = getField().getAnnotation(PrimaryKeyColumn.class);

		return anno != null && anno.type() == PrimaryKeyType.PARTITIONED;
	}

	@Override
	public boolean isClusterKeyColumn() {

		PrimaryKeyColumn anno = getField().getAnnotation(PrimaryKeyColumn.class);

		return anno != null && anno.type() == PrimaryKeyType.CLUSTERED;
	}

	@Override
	public boolean isPrimaryKeyColumn() {
		return getField().isAnnotationPresent(PrimaryKeyColumn.class);
	}

	@Override
	protected Association<CassandraPersistentProperty> createAssociation() {
		return new Association<CassandraPersistentProperty>(this, null);
	}

	DataType resolvePrimitiveType(DataType.Name typeName) {
		DataType dataType = CassandraSimpleTypes.resolvePrimitive(typeName);
		if (dataType == null) {
			throw new InvalidDataAccessApiUsageException(
					"only primitive types are allowed inside collections for the property  '" + this.getName() + "' type is '"
							+ this.getType() + "' in the entity " + this.getOwner().getName());
		}
		return dataType;
	}

	DataType autodetectPrimitiveType(Class<?> javaType) {
		DataType dataType = CassandraSimpleTypes.autodetectPrimitive(javaType);
		if (dataType == null) {
			throw new InvalidDataAccessApiUsageException(
					"only primitive types are allowed inside collections for the property  '" + this.getName() + "' type is '"
							+ this.getType() + "' in the entity " + this.getOwner().getName());
		}
		return dataType;
	}

	void ensureTypeArguments(int args, int expected) {
		if (args != expected) {
			throw new InvalidDataAccessApiUsageException("expected " + expected + " of typed arguments for the property  '"
					+ this.getName() + "' type is '" + this.getType() + "' in the entity " + this.getOwner().getName());
		}
	}
}
