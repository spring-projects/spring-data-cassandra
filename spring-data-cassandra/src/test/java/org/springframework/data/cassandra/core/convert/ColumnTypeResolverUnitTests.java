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

import static org.assertj.core.api.Assertions.*;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.util.ClassTypeInformation;

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataTypes;

/**
 * Unit tests for {@link DefaultColumnTypeResolver}.
 *
 * @author Mark Paluch
 */
public class ColumnTypeResolverUnitTests {

	CassandraMappingContext mappingContext = new CassandraMappingContext();
	ColumnTypeResolver resolver = new DefaultColumnTypeResolver(mappingContext);

	@Test // DATACASS-743
	public void shouldResolveSimpleType() {

		assertThat(resolver.resolve("foo").getType()).isEqualTo(String.class);
		assertThat(resolver.resolve(ClassTypeInformation.from(String.class)).getType()).isEqualTo(String.class);
		assertThat(resolver.resolve(ClassTypeInformation.from(String.class)).getDataType()).isEqualTo(DataTypes.TEXT);

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("name")).getType()).isEqualTo(String.class);
		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("name")).getDataType()).isEqualTo(DataTypes.TEXT);
	}

	@Test // DATACASS-743
	public void shouldResolveEnumType() {

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
	public void shouldConsiderCassandraType() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumAsInt")).getType()).isEqualTo(Integer.class);
		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumAsInt")).getDataType())
				.isEqualTo(DataTypes.INT);
	}

	@Test // DATACASS-743
	public void shouldResolveSimpleListType() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("names")).getType()).isEqualTo(List.class);
		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("names")).getDataType())
				.isEqualTo(DataTypes.listOf(DataTypes.TEXT));
	}

	@Test // DATACASS-743
	public void shouldResolveListOfEnumType() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumsAsString")).getType()).isEqualTo(List.class);
		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumsAsString")).getDataType())
				.isEqualTo(DataTypes.listOf(DataTypes.TEXT));
	}

	@Test // DATACASS-743
	public void shouldConsiderListWithCassandraType() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumsAsInt")).getType()).isEqualTo(List.class);
		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumsAsInt")).getDataType())
				.isEqualTo(DataTypes.listOf(DataTypes.INT));
	}

	@Test // DATACASS-743
	public void shouldResolveSimpleSetType() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("nameSet")).getType()).isEqualTo(Set.class);
		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("nameSet")).getDataType())
				.isEqualTo(DataTypes.setOf(DataTypes.TEXT));
	}

	@Test // DATACASS-743
	public void shouldResolveSetOfEnumType() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumSetAsString")).getType())
				.isEqualTo(Set.class);
		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumSetAsString")).getDataType())
				.isEqualTo(DataTypes.setOf(DataTypes.TEXT));
	}

	@Test // DATACASS-743
	public void shouldConsiderSetWithCassandraType() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumSetAsInt")).getType()).isEqualTo(Set.class);
		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumSetAsInt")).getDataType())
				.isEqualTo(DataTypes.setOf(DataTypes.INT));
	}

	@Test // DATACASS-743
	public void shouldResolveSimpleMapType() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("nameMap")).getType()).isEqualTo(Map.class);
		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("nameMap")).getDataType())
				.isEqualTo(DataTypes.mapOf(DataTypes.TEXT, DataTypes.TEXT));
	}

	@Test // DATACASS-743
	public void shouldResolveMapOfEnumType() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumMapAsString")).getType())
				.isEqualTo(Map.class);
		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumMapAsString")).getDataType())
				.isEqualTo(DataTypes.mapOf(DataTypes.TEXT, DataTypes.TEXT));
	}

	@Test // DATACASS-743
	public void shouldConsiderMapWithCassandraType() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumMapAsInt")).getType()).isEqualTo(Map.class);
		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("enumMapAsInt")).getDataType())
				.isEqualTo(DataTypes.mapOf(DataTypes.INT, DataTypes.TEXT));
	}

	@Test // DATACASS-743
	public void shouldReportEmptyTupleType() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Person.class);

		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("tupleValue")).getType())
				.isEqualTo(TupleValue.class);
		assertThat(resolver.resolve(entity.getRequiredPersistentProperty("tupleValue")).getDataType())
				.isEqualTo(DataTypes.tupleOf());
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
	}

	enum MyEnum {
		INSTANCE;
	}
}
