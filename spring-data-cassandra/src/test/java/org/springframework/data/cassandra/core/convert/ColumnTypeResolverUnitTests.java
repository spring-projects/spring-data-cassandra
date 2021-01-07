/*
 * Copyright 2020-2021 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Frozen;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.util.ClassTypeInformation;

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

/**
 * Unit tests for {@link DefaultColumnTypeResolver}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
public class ColumnTypeResolverUnitTests {

	private CassandraMappingContext mappingContext = new CassandraMappingContext();
	private ColumnTypeResolver resolver = new DefaultColumnTypeResolver(mappingContext,
			SchemaFactory.ShallowUserTypeResolver.INSTANCE, () -> CodecRegistry.DEFAULT,
			mappingContext::getCustomConversions);

	@Test // DATACASS-743
	void shouldResolveSimpleType() {

		assertThat(resolver.resolve("foo").getType()).isEqualTo(String.class);
		assertThat(resolver.resolve(ClassTypeInformation.from(String.class)).getType()).isEqualTo(String.class);
		assertThat(resolver.resolve(ClassTypeInformation.from(String.class)).getDataType()).isEqualTo(DataTypes.TEXT);

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("name")).getType()).isEqualTo(String.class);
		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("name")).getDataType()).isEqualTo(DataTypes.TEXT);
	}

	@Test // DATACASS-743
	void shouldResolveEnumType() {

		assertThat(resolver.resolve(MyEnum.INSTANCE).getType()).isEqualTo(String.class);
		assertThat(resolver.resolve(ClassTypeInformation.from(MyEnum.class)).getType()).isEqualTo(String.class);
		assertThat(resolver.resolve(ClassTypeInformation.from(MyEnum.class)).getDataType()).isEqualTo(DataTypes.TEXT);

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumAsString")).getType())
				.isEqualTo(String.class);
		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumAsString")).getDataType())
				.isEqualTo(DataTypes.TEXT);
	}

	@Test // DATACASS-743
	void shouldConsiderCassandraType() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumAsInt")).getType()).isEqualTo(Integer.class);
		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumAsInt")).getDataType())
				.isEqualTo(DataTypes.INT);
	}

	@Test // DATACASS-743
	void shouldResolveSimpleListType() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("names")).getType()).isEqualTo(List.class);
		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("names")).getDataType())
				.isEqualTo(DataTypes.listOf(DataTypes.TEXT));
	}

	@Test // DATACASS-743
	void shouldResolveListOfEnumType() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumsAsString")).getType()).isEqualTo(List.class);
		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumsAsString")).getDataType())
				.isEqualTo(DataTypes.listOf(DataTypes.TEXT));
	}

	@Test // DATACASS-743
	void shouldConsiderListWithCassandraType() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumsAsInt")).getType()).isEqualTo(List.class);
		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumsAsInt")).getDataType())
				.isEqualTo(DataTypes.listOf(DataTypes.INT));
	}

	@Test // DATACASS-743
	void shouldResolveSimpleSetType() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("nameSet")).getType()).isEqualTo(Set.class);
		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("nameSet")).getDataType())
				.isEqualTo(DataTypes.setOf(DataTypes.TEXT));
	}

	@Test // DATACASS-743
	void shouldResolveSetOfEnumType() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumSetAsString")).getType())
				.isEqualTo(Set.class);
		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumSetAsString")).getDataType())
				.isEqualTo(DataTypes.setOf(DataTypes.TEXT));
	}

	@Test // DATACASS-743
	void shouldConsiderSetWithCassandraType() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumSetAsInt")).getType()).isEqualTo(Set.class);
		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumSetAsInt")).getDataType())
				.isEqualTo(DataTypes.setOf(DataTypes.INT));
	}

	@Test // DATACASS-743
	void shouldResolveSimpleMapType() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("nameMap")).getType()).isEqualTo(Map.class);
		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("nameMap")).getDataType())
				.isEqualTo(DataTypes.mapOf(DataTypes.TEXT, DataTypes.TEXT));
	}

	@Test // DATACASS-743
	void shouldResolveMapOfEnumType() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumMapAsString")).getType())
				.isEqualTo(Map.class);
		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumMapAsString")).getDataType())
				.isEqualTo(DataTypes.mapOf(DataTypes.TEXT, DataTypes.TEXT));
	}

	@Test // DATACASS-743
	void shouldConsiderMapWithCassandraType() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumMapAsInt")).getType()).isEqualTo(Map.class);
		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumMapAsInt")).getDataType())
				.isEqualTo(DataTypes.mapOf(DataTypes.INT, DataTypes.TEXT));
	}

	@Test // DATACASS-743
	void shouldReportEmptyTupleType() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		CassandraColumnType columnType = resolver.resolve(entity.getRequiredPersistentProperty("tupleValue"));
		assertThat(columnType.getType()).isEqualTo(TupleValue.class);

		assertThatThrownBy(columnType::getDataType).isInstanceOf(MappingException.class);
	}

	@Test // DATACASS-375, DATACASS-743
	void UuidshouldMapToUUIDByDefault() {

		CassandraPersistentProperty uuidProperty = mappingContext.getRequiredPersistentEntity(TypeWithUUIDColumn.class)
				.getRequiredPersistentProperty("uuid");
		CassandraPersistentProperty timeUUIDProperty = mappingContext.getRequiredPersistentEntity(TypeWithUUIDColumn.class)
				.getRequiredPersistentProperty("timeUUID");

		assertThat(resolver.resolve(uuidProperty).getDataType()).isEqualTo(DataTypes.UUID);
		assertThat(resolver.resolve(timeUUIDProperty).getDataType()).isEqualTo(DataTypes.TIMEUUID);
	}

	@Test // DATACASS-465
	void listPropertyWithFrozenAnnotation() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		DataType dataType = resolver.resolve(entity.getRequiredPersistentProperty("frozenList")).getDataType();
		assertThat(dataType).isInstanceOf(ListType.class);
		assertThat(((ListType) dataType).isFrozen()).describedAs("The list itself should be frozen").isTrue();
	}

	@Test // DATACASS-465
	void listPropertyWithFrozenAnnotationOnElement() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		DataType dataType = resolver.resolve(entity.getRequiredPersistentProperty("frozenListContent")).getDataType();

		assertThat(dataType).isInstanceOf(ListType.class);
		assertThat(((ListType) dataType).isFrozen()).describedAs("The collection itself should not be frozen.").isFalse();

		DataType elementType = ((ListType) dataType).getElementType();
		assertThat(elementType).isInstanceOf(com.datastax.oss.driver.api.core.type.UserDefinedType.class);
		assertThat(((com.datastax.oss.driver.api.core.type.UserDefinedType) elementType).isFrozen())
				.describedAs("The element type should be frozen").isTrue();
	}

	@Test // DATACASS-465
	void setPropertyWithFrozenAnnotation() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		DataType dataType = resolver.resolve(entity.getRequiredPersistentProperty("frozenSet")).getDataType();
		assertThat(dataType).isInstanceOf(SetType.class);
		assertThat(((SetType) dataType).isFrozen()).describedAs("The set itself should be frozen").isTrue();
	}

	@Test // DATACASS-465
	void setPropertyWithFrozenAnnotationOnElement() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		DataType dataType = resolver.resolve(entity.getRequiredPersistentProperty("frozenSetContent")).getDataType();

		assertThat(dataType).isInstanceOf(SetType.class);
		assertThat(((SetType) dataType).isFrozen()).describedAs("The collection itself should not be frozen.").isFalse();

		DataType elementType = ((SetType) dataType).getElementType();
		assertThat(elementType).isInstanceOf(com.datastax.oss.driver.api.core.type.UserDefinedType.class);
		assertThat(((com.datastax.oss.driver.api.core.type.UserDefinedType) elementType).isFrozen())
				.describedAs("The element type should be frozen").isTrue();
	}

	@Test // DATACASS-465
	void mapPropertyWithFrozenAnnotationOnKey() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		DataType dataType = resolver.resolve(entity.getRequiredPersistentProperty("frozenMapKey")).getDataType();

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(dataType).isInstanceOf(MapType.class);
			softly.assertThat(((MapType) dataType).isFrozen()).describedAs("The collection itself should not be frozen.")
					.isFalse();

			DataType keyType = ((MapType) dataType).getKeyType();
			softly.assertThat(keyType).isInstanceOf(com.datastax.oss.driver.api.core.type.UserDefinedType.class);
			softly.assertThat(((com.datastax.oss.driver.api.core.type.UserDefinedType) keyType).isFrozen())
					.describedAs("The key type should be frozen").isTrue();

			DataType valueType = ((MapType) dataType).getValueType();
			softly.assertThat(valueType).isInstanceOf(com.datastax.oss.driver.api.core.type.UserDefinedType.class);
			softly.assertThat(((com.datastax.oss.driver.api.core.type.UserDefinedType) valueType).isFrozen())
					.describedAs("The value type should not be frozen").isFalse();
		});
	}

	@Test // DATACASS-465
	void mapPropertyWithFrozenAnnotationOnValue() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		DataType dataType = resolver.resolve(entity.getRequiredPersistentProperty("frozenMapValue")).getDataType();

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(dataType).isInstanceOf(MapType.class);
			softly.assertThat(((MapType) dataType).isFrozen()).describedAs("The collection itself should not be frozen.")
					.isFalse();

			DataType keyType = ((MapType) dataType).getKeyType();
			softly.assertThat(keyType).isInstanceOf(com.datastax.oss.driver.api.core.type.UserDefinedType.class);
			softly.assertThat(((com.datastax.oss.driver.api.core.type.UserDefinedType) keyType).isFrozen())
					.describedAs("The key type should not be frozen").isFalse();

			DataType valueType = ((MapType) dataType).getValueType();
			softly.assertThat(valueType).isInstanceOf(com.datastax.oss.driver.api.core.type.UserDefinedType.class);
			softly.assertThat(((com.datastax.oss.driver.api.core.type.UserDefinedType) valueType).isFrozen())
					.describedAs("The value type should be frozen").isTrue();
		});
	}

	static class Person {

		String name;

		MyEnum enumAsString;

		@CassandraType(type = CassandraType.Name.INT) MyEnum enumAsInt;

		List<String> names;

		List<MyEnum> enumsAsString;

		@CassandraType(type = CassandraType.Name.LIST, typeArguments = CassandraType.Name.INT) List<MyEnum> enumsAsInt;

		Set<String> nameSet;

		EnumSet<MyEnum> enumSetAsString;

		@CassandraType(type = CassandraType.Name.SET, typeArguments = CassandraType.Name.INT) EnumSet<MyEnum> enumSetAsInt;

		Map<String, String> nameMap;

		Map<MyEnum, MyEnum> enumMapAsString;

		@CassandraType(type = CassandraType.Name.MAP,
				typeArguments = { CassandraType.Name.INT, CassandraType.Name.TEXT }) Map<MyEnum, MyEnum> enumMapAsInt;

		TupleValue tupleValue;

		@Frozen List<String> frozenList;
		List<@Frozen MyUdt> frozenListContent;

		@Frozen Set<String> frozenSet;
		Set<@Frozen MyUdt> frozenSetContent;

		Map<@Frozen MyUdt, MyUdt> frozenMapKey;

		Map<MyUdt, @Frozen MyUdt> frozenMapValue;

		@UserDefinedType
		private static class MyUdt {
			String one;
			Integer two;
		}
	}

	private static class TypeWithUUIDColumn {

		UUID uuid;

		@CassandraType(type = CassandraType.Name.TIMEUUID) UUID timeUUID;
	}

	enum MyEnum {
		INSTANCE;
	}
}
