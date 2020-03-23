/*
 * Copyright 2020 the original author or authors.
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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.StreamSupport;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraSimpleTypeHolder;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.UserTypeResolver;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

/**
 * Default {@link ColumnTypeResolver} implementation backed by {@link CustomConversions} and {@link CodecRegistry}.
 *
 * @author Mark Paluch
 * @since 3.0
 */
class DefaultColumnTypeResolver implements ColumnTypeResolver {

	private final CassandraMappingContext mappingContext;
	private final UserTypeResolver userTypeResolver;
	private final CodecRegistry codecRegistry;
	private CustomConversions customConversions;

	public DefaultColumnTypeResolver(CassandraMappingContext mappingContext) {
		this.mappingContext = mappingContext;
		this.userTypeResolver = mappingContext.getUserTypeResolver();
		this.codecRegistry = mappingContext.getCodecRegistry();
		this.customConversions = mappingContext.getCustomConversions();
	}

	public void setCustomConversions(CustomConversions customConversions) {
		this.customConversions = customConversions;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.convert.ColumnTypeResolver#resolve(org.springframework.data.util.TypeInformation)
	 */
	@Override
	public CassandraColumnType resolve(TypeInformation<?> typeInformation) {

		Optional<Class<?>> writeTarget = customConversions.getCustomWriteTarget(typeInformation.getType());

		return writeTarget.map(it -> {
			return createCassandraTypeDescriptor(tryResolve(it), ClassTypeInformation.from(it));
		}).orElseGet(() -> {

			if (typeInformation.getType().isEnum()) {
				return ColumnType.create(String.class, DataTypes.TEXT);
			}

			return createCassandraTypeDescriptor(typeInformation);
		});
	}

	private DataType tryResolve(Class<?> type) {

		if (TupleValue.class.isAssignableFrom(type)) {
			return DataTypes.tupleOf();
		}

		if (UdtValue.class.isAssignableFrom(type)) {
			return UnknownUserDefinedType.INSTANCE;
		}

		return codecRegistry.codecFor(type).getCqlType();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.convert.ColumnTypeResolver#resolve(org.springframework.data.cassandra.core.mapping.CassandraType)
	 */
	@Override
	public CassandraColumnType resolve(CassandraType annotation) {

		CassandraType.Name type = annotation.type();

		switch (type) {
			case MAP:
				assertTypeArguments(annotation.typeArguments().length, 2);

				CassandraColumnType keyType = createCassandraTypeDescriptor(
						CassandraSimpleTypeHolder.getDataTypeFor(annotation.typeArguments()[0]));
				CassandraColumnType valueType = createCassandraTypeDescriptor(
						CassandraSimpleTypeHolder.getDataTypeFor(annotation.typeArguments()[1]));

				return ColumnType.mapOf(keyType, valueType);

			case LIST:
			case SET:
				assertTypeArguments(annotation.typeArguments().length, 1);

				DataType componentType = annotation.typeArguments()[0] == CassandraType.Name.UDT
						? getUserType(annotation.userTypeName())
						: CassandraSimpleTypeHolder.getDataTypeFor(annotation.typeArguments()[0]);

				if (type == CassandraType.Name.SET) {
					return ColumnType.setOf(createCassandraTypeDescriptor(componentType));
				}

				return ColumnType.listOf(createCassandraTypeDescriptor(componentType));

			case TUPLE:

				DataType[] dataTypes = Arrays.stream(annotation.typeArguments()).map(CassandraSimpleTypeHolder::getDataTypeFor)
						.toArray(DataType[]::new);

				return ColumnType.tupleOf(DataTypes.tupleOf(dataTypes));
			case UDT:

				return createCassandraTypeDescriptor(getUserType(annotation.userTypeName()));
			default:
				return createCassandraTypeDescriptor(CassandraSimpleTypeHolder.getDataTypeFor(type));
		}
	}

	/*
	 *  (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.convert.ColumnTypeResolver#resolve(java.lang.Object)
	 */
	@Override
	public ColumnType resolve(@Nullable Object value) {

		if (value != null) {

			ClassTypeInformation<?> typeInformation = ClassTypeInformation.from(value.getClass());

			Optional<Class<?>> writeTarget = customConversions.getCustomWriteTarget(typeInformation.getType());

			return writeTarget.map(it -> {
				return (ColumnType) createCassandraTypeDescriptor(tryResolve(it), typeInformation);
			}).orElseGet(() -> {

				if (typeInformation.getType().isEnum()) {
					return ColumnType.create(String.class, DataTypes.TEXT);
				}

				if (value instanceof Map) {
					return ColumnType.mapOf(DefaultColumnType.OBJECT, DefaultColumnType.OBJECT);
				}

				if (value instanceof List) {
					return ColumnType.listOf(DefaultColumnType.OBJECT);
				}

				if (value instanceof Set) {
					return ColumnType.listOf(DefaultColumnType.OBJECT);
				}

				if (value instanceof UdtValue) {
					return ColumnType.udtOf(((UdtValue) value).getType());
				}

				if (value instanceof TupleValue) {
					return ColumnType.tupleOf(((TupleValue) value).getType());
				}

				BasicCassandraPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(typeInformation);

				if (persistentEntity != null) {

					if (persistentEntity.isUserDefinedType() || persistentEntity.isTupleType()) {
						return resolve(persistentEntity.getTypeInformation());
					}
				}

				return ColumnType.create(typeInformation.getType());
			});
		}

		return DefaultColumnType.OBJECT;
	}

	private CassandraColumnType createCassandraTypeDescriptor(DataType dataType) {

		GenericType<Object> javaType = codecRegistry.codecFor(dataType).getJavaType();
		return ColumnType.create(javaType.getRawType(), dataType);
	}

	private CassandraColumnType createCassandraTypeDescriptor(DataType dataType, TypeInformation<?> typeInformation) {

		if (typeInformation.isCollectionLike() || typeInformation.isMap()) {

			if (dataType instanceof ListType) {

				TypeInformation<?> component = typeInformation.getComponentType();
				DataType elementType = ((ListType) dataType).getElementType();

				if (component != null) {
					return ColumnType.listOf(createCassandraTypeDescriptor(elementType, component));
				}

				Class<?> componentType = resolveToJavaType(elementType);
				return ColumnType.listOf(ColumnType.create(componentType, elementType));
			}

			if (dataType instanceof SetType) {

				TypeInformation<?> component = typeInformation.getComponentType();
				DataType elementType = ((SetType) dataType).getElementType();

				if (component != null) {
					return ColumnType.setOf(createCassandraTypeDescriptor(elementType, component));
				}

				Class<?> componentType = resolveToJavaType(elementType);
				return ColumnType.setOf(ColumnType.create(componentType, elementType));
			}

			if (dataType instanceof MapType) {

				TypeInformation<?> mapKeyType = typeInformation.getComponentType();
				TypeInformation<?> mapValueType = typeInformation.getMapValueType();

				MapType mapType = (MapType) dataType;

				CassandraColumnType keyDescriptor = null;
				CassandraColumnType valueDescriptor = null;

				if (mapKeyType != null) {
					keyDescriptor = createCassandraTypeDescriptor(mapType.getKeyType(), mapKeyType);
				}

				if (mapValueType != null) {
					valueDescriptor = createCassandraTypeDescriptor(mapType.getValueType(), mapValueType);
				}

				if (keyDescriptor == null) {
					keyDescriptor = ColumnType.create(resolveToJavaType(mapType.getKeyType()), mapType.getKeyType());
				}

				if (valueDescriptor == null) {
					valueDescriptor = ColumnType.create(resolveToJavaType(mapType.getValueType()), mapType.getValueType());
				}

				return ColumnType.mapOf(keyDescriptor, valueDescriptor);
			}
		}

		return new DefaultCassandraColumnType(typeInformation, dataType);
	}

	private CassandraColumnType createCassandraTypeDescriptor(TypeInformation<?> typeInformation) {

		if (List.class.isAssignableFrom(typeInformation.getType())) {
			return ColumnType.listOf(resolve(typeInformation.getRequiredComponentType()));
		}

		if (Set.class.isAssignableFrom(typeInformation.getType())) {
			return ColumnType.setOf(resolve(typeInformation.getRequiredComponentType()));
		}

		if (typeInformation.isMap()) {
			return ColumnType.mapOf(resolve(typeInformation.getRequiredComponentType()),
					resolve(typeInformation.getRequiredMapValueType()));
		}

		BasicCassandraPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(typeInformation);

		if (persistentEntity != null) {

			if (persistentEntity.isUserDefinedType()) {
				return new DefaultCassandraColumnType(typeInformation, persistentEntity.getUserType());
			}

			if (persistentEntity.isTupleType()) {

				DataType[] componentTypes = StreamSupport.stream(persistentEntity.spliterator(), false) //
						.map(this::resolve) //
						.map(CassandraColumnType::getDataType) //
						.toArray(DataType[]::new);

				return new DefaultCassandraColumnType(typeInformation, DataTypes.tupleOf(componentTypes));
			}

			return new UnresolvableCassandraType(typeInformation);
		}

		return new DefaultCassandraColumnType(typeInformation, tryResolve(typeInformation.getType()));
	}

	private Class<?> resolveToJavaType(DataType dataType) {
		TypeCodec<Object> codec = codecRegistry.codecFor(dataType);
		return codec.getJavaType().getRawType();
	}

	private DataType getUserType(String userTypeName) {

		UserDefinedType type = userTypeResolver.resolveType(CqlIdentifier.fromCql(userTypeName));

		if (type == null) {
			throw new IllegalArgumentException(String.format("Cannot resolve UserDefinedType for [%s]", userTypeName));
		}

		return type;
	}

	private void assertTypeArguments(int args, int expected) {

		if (args != expected) {
			throw new InvalidDataAccessApiUsageException(
					String.format("Expected [%d] type arguments actual was [%d]", expected, args));
		}
	}

	enum UnknownUserDefinedType implements com.datastax.oss.driver.api.core.type.UserDefinedType {
		INSTANCE;

		UnknownUserDefinedType() {}

		/*
		 * (non-Javadoc)
		 * @see com.datastax.oss.driver.api.core.type.UserDefinedType#getKeyspace()
		 */
		@Override
		public CqlIdentifier getKeyspace() {
			return null;
		}

		/*
		 * (non-Javadoc)
		 * @see com.datastax.oss.driver.api.core.type.UserDefinedType#getName()
		 */
		@Override
		public CqlIdentifier getName() {
			return CqlIdentifier.fromCql("unknown");
		}

		/*
		 * (non-Javadoc)
		 * @see com.datastax.oss.driver.api.core.type.UserDefinedType#isFrozen()
		 */
		@Override
		public boolean isFrozen() {
			return false;
		}

		/*
		 * (non-Javadoc)
		 * @see com.datastax.oss.driver.api.core.type.UserDefinedType#getFieldNames()
		 */
		@Override
		public List<CqlIdentifier> getFieldNames() {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		/*
		 * (non-Javadoc)
		 * @see com.datastax.oss.driver.api.core.type.UserDefinedType#firstIndexOf(com.datastax.oss.driver.api.core.CqlIdentifier)
		 */
		@Override
		public int firstIndexOf(CqlIdentifier id) {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		/*
		 * (non-Javadoc)
		 * @see com.datastax.oss.driver.api.core.type.UserDefinedType#firstIndexOf(java.lang.String)
		 */
		@Override
		public int firstIndexOf(String name) {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		/*
		 * (non-Javadoc)
		 * @see com.datastax.oss.driver.api.core.type.UserDefinedType#getFieldTypes()
		 */
		@Override
		public List<DataType> getFieldTypes() {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		/*
		 * (non-Javadoc)
		 * @see com.datastax.oss.driver.api.core.type.UserDefinedType#copy(boolean)
		 */
		@Override
		public com.datastax.oss.driver.api.core.type.UserDefinedType copy(boolean newFrozen) {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		/*
		 * (non-Javadoc)
		 * @see com.datastax.oss.driver.api.core.type.UserDefinedType#newValue()
		 */
		@Override
		public UdtValue newValue() {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		/*
		 * (non-Javadoc)
		 * @see com.datastax.oss.driver.api.core.type.UserDefinedType#newValue(java.lang.Object[])
		 */
		@Override
		public UdtValue newValue(Object... fields) {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		/*
		 * (non-Javadoc)
		 * @see com.datastax.oss.driver.api.core.type.UserDefinedType#getAttachmentPoint()
		 */
		@Override
		public AttachmentPoint getAttachmentPoint() {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		/*
		 * (non-Javadoc)
		 * @see com.datastax.oss.driver.api.core.detach.Detachable#isDetached()
		 */
		@Override
		public boolean isDetached() {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		/*
		 * (non-Javadoc)
		 * @see com.datastax.oss.driver.api.core.detach.Detachable#attach(com.datastax.oss.driver.api.core.detach.AttachmentPoint)
		 */
		@Override
		public void attach(AttachmentPoint attachmentPoint) {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}
	}

	static class UnresolvableCassandraType extends DefaultCassandraColumnType {

		public UnresolvableCassandraType(TypeInformation<?> type, ColumnType... parameters) {
			super(type, null, parameters);
		}

		public UnresolvableCassandraType(Class<?> type, ColumnType... parameters) {
			super(type, null, parameters);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.convert.DefaultCassandraColumnType#getDataType()
		 */
		@Override
		public DataType getDataType() {
			throw new MappingException(String.format("Cannot resolve DataType for %s", getType().getName()));
		}
	}
}
