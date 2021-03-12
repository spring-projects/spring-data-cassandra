/*
 * Copyright 2018-2021 the original author or authors.
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
import static org.mockito.Mockito.*;
import static org.springframework.data.cassandra.test.util.RowMockUtil.*;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.ReadOnlyProperty;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Embedded;
import org.springframework.data.cassandra.core.mapping.Frozen;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;
import org.springframework.data.cassandra.core.mapping.UserTypeResolver;
import org.springframework.data.cassandra.support.UserDefinedTypeBuilder;
import org.springframework.data.cassandra.test.util.RowMockUtil;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;

/**
 * Unit tests for UDT through {@link MappingCassandraConverter}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class MappingCassandraConverterUDTUnitTests {

	@Mock UserTypeResolver userTypeResolver;

	private com.datastax.oss.driver.api.core.type.UserDefinedType manufacturer = UserDefinedTypeBuilder
			.forName("manufacturer")
			.withField("name", DataTypes.TEXT).withField("displayname", DataTypes.TEXT).build();
	private com.datastax.oss.driver.api.core.type.UserDefinedType currency = UserDefinedTypeBuilder.forName("mycurrency")
			.withField("currency", DataTypes.TEXT).build();
	private com.datastax.oss.driver.api.core.type.UserDefinedType withnullableembeddedtype = UserDefinedTypeBuilder
			.forName("withnullableembeddedtype").withField("value", DataTypes.TEXT).withField("firstname", DataTypes.TEXT)
			.withField("age", DataTypes.INT).build();
	private com.datastax.oss.driver.api.core.type.UserDefinedType withprefixednullableembeddedtype = UserDefinedTypeBuilder
			.forName("withnullableembeddedtype").withField("value", DataTypes.TEXT)
			.withField("prefixfirstname", DataTypes.TEXT).withField("prefixage", DataTypes.INT).build();

	private Row rowMock;

	private CassandraMappingContext mappingContext;
	private MappingCassandraConverter mappingCassandraConverter;

	@BeforeEach
	void setUp() {

		mappingContext = new CassandraMappingContext();
		mappingContext.setUserTypeResolver(userTypeResolver);

		mappingCassandraConverter = new MappingCassandraConverter(mappingContext);
		mappingCassandraConverter.afterPropertiesSet();

		when(userTypeResolver.resolveType(CqlIdentifier.fromCql("manufacturer"))).thenReturn(manufacturer);
		when(userTypeResolver.resolveType(CqlIdentifier.fromCql("currency"))).thenReturn(currency);
		when(userTypeResolver.resolveType(CqlIdentifier.fromCql("withnullableembeddedtype")))
				.thenReturn(withnullableembeddedtype);
		when(userTypeResolver.resolveType(CqlIdentifier.fromCql("withprefixednullableembeddedtype")))
				.thenReturn(withprefixednullableembeddedtype);
	}

	@Test // DATACASS-487, DATACASS-623
	void shouldReadMappedUdtInMap() {

		UdtValue key = manufacturer.newValue().setString("name", "a good one").setString("displayname", "my displayName");
		UdtValue value1 = currency.newValue().setString("currency", "EUR");
		UdtValue value2 = currency.newValue().setString("currency", "USD");

		Map<UdtValue, List<UdtValue>> map = new HashMap<>();

		map.put(key, Arrays.asList(value1, value2));

		rowMock = RowMockUtil
				.newRowMock(column("acceptedCurrencies", map, DataTypes.mapOf(manufacturer, DataTypes.listOf(currency))));

		Supplier supplier = mappingCassandraConverter.read(Supplier.class, rowMock);

		assertThat(supplier.getAcceptedCurrencies()).isNotEmpty();

		List<Currency> currencies = supplier.getAcceptedCurrencies().get(new Manufacturer("a good one", "my displayName"));

		assertThat(currencies).contains(new Currency("EUR"), new Currency("USD"));
	}

	@Test // DATACASS-487, DATACASS-623
	void shouldWriteMappedUdtInMap() {

		Map<Manufacturer, List<Currency>> currencies = Collections.singletonMap(new Manufacturer("a good one", "foo"),
				Arrays.asList(new Currency("EUR"), new Currency("USD")));

		Supplier supplier = new Supplier(currencies);

		Map<CqlIdentifier, Object> insert = new LinkedHashMap<>();

		mappingCassandraConverter.write(supplier, insert);

		Map<UdtValue, List<UdtValue>> acceptedcurrencies = (Map) insert.get(CqlIdentifier.fromCql("acceptedcurrencies"));

		assertThat(acceptedcurrencies).hasSize(1);

		Map.Entry<UdtValue, List<UdtValue>> entry = acceptedcurrencies.entrySet().iterator().next();

		assertThat(entry.getKey().getFormattedContents()).contains("{name:'a good one',displayname:NULL}");
		assertThat(entry.getValue()).hasSize(2) //
				.extracting(UdtValue::getFormattedContents) //
				.contains("{currency:'EUR'}", "{currency:'USD'}");
	}

	@Test // DATACASS-167
	void writeFlattensEmbeddedType() {

		OuterWithNullableEmbeddedType entity = new OuterWithNullableEmbeddedType();
		entity.id = "id-1";
		entity.udtValue = new WithNullableEmbeddedType();
		entity.udtValue.value = "value-string";
		entity.udtValue.nested = new EmbeddedWithSimpleTypes();
		entity.udtValue.nested.firstname = "fn";
		entity.udtValue.nested.age = 30;

		Map<CqlIdentifier, Object> sink = new LinkedHashMap<>();

		mappingCassandraConverter.write(entity, sink);

		assertThat(sink).containsEntry(CqlIdentifier.fromInternal("id"), "id-1");
		assertThat((UdtValue) sink.get(CqlIdentifier.fromInternal("udtvalue"))).extracting(UdtValue::getFormattedContents)
				.isEqualTo("{value:'value-string',firstname:'fn',age:30}");
	}

	@Test // DATACASS-167
	void writeNullEmbeddedType() {

		OuterWithNullableEmbeddedType entity = new OuterWithNullableEmbeddedType();
		entity.id = "id-1";
		entity.udtValue = new WithNullableEmbeddedType();
		entity.udtValue.value = "value-string";
		entity.udtValue.nested = null;

		Map<CqlIdentifier, Object> sink = new LinkedHashMap<>();

		mappingCassandraConverter.write(entity, sink);

		assertThat(sink).containsEntry(CqlIdentifier.fromInternal("id"), "id-1");
		assertThat((UdtValue) sink.get(CqlIdentifier.fromInternal("udtvalue"))).extracting(UdtValue::getFormattedContents)
				.isEqualTo("{value:'value-string',firstname:NULL,age:NULL}");
	}

	@Test // DATACASS-167
	void writePrefixesEmbeddedType() {

		OuterWithPrefixedNullableEmbeddedType entity = new OuterWithPrefixedNullableEmbeddedType();
		entity.id = "id-1";
		entity.udtValue = new WithPrefixedNullableEmbeddedType();
		entity.udtValue.value = "value-string";
		entity.udtValue.nested = new EmbeddedWithSimpleTypes();
		entity.udtValue.nested.firstname = "fn";
		entity.udtValue.nested.age = 30;

		Map<CqlIdentifier, Object> sink = new LinkedHashMap<>();

		mappingCassandraConverter.write(entity, sink);

		assertThat(sink).containsEntry(CqlIdentifier.fromInternal("id"), "id-1");
		assertThat((UdtValue) sink.get(CqlIdentifier.fromInternal("udtvalue"))).extracting(UdtValue::getFormattedContents)
				.isEqualTo("{value:'value-string',prefixfirstname:'fn',prefixage:30}");
	}

	@Test // DATACASS-167
	void readEmbeddedType() {

		UdtValue udtValue = withnullableembeddedtype.newValue().setString("value", "value-string")
				.setString("firstname", "fn").setInt("age", 30);

		rowMock = RowMockUtil.newRowMock(column("id", "id-1", DataTypes.TEXT),
				column("udtvalue", udtValue, withnullableembeddedtype));

		OuterWithNullableEmbeddedType target = mappingCassandraConverter.read(OuterWithNullableEmbeddedType.class, rowMock);
		assertThat(target.getId()).isEqualTo("id-1");
		assertThat(target.udtValue).isNotNull();
		assertThat(target.udtValue.value).isEqualTo("value-string");
		assertThat(target.udtValue.nested.firstname).isEqualTo("fn");
		assertThat(target.udtValue.nested.age).isEqualTo(30);
	}

	@Test // DATACASS-167
	void readPrefixedEmbeddedType() {

		UdtValue udtValue = withprefixednullableembeddedtype.newValue().setString("value", "value-string")
				.setString("prefixfirstname", "fn").setInt("prefixage", 30);

		rowMock = RowMockUtil.newRowMock(column("id", "id-1", DataTypes.TEXT),
				column("udtvalue", udtValue, withprefixednullableembeddedtype));

		OuterWithPrefixedNullableEmbeddedType target = mappingCassandraConverter
				.read(OuterWithPrefixedNullableEmbeddedType.class, rowMock);
		assertThat(target.getId()).isEqualTo("id-1");
		assertThat(target.udtValue).isNotNull();
		assertThat(target.udtValue.value).isEqualTo("value-string");
		assertThat(target.udtValue.nested.firstname).isEqualTo("fn");
		assertThat(target.udtValue.nested.age).isEqualTo(30);
	}

	@Test // #1098
	void shouldWriteMapWithTypeHintToUdtValue() {

		when(userTypeResolver.resolveType(CqlIdentifier.fromCql("udt"))).thenReturn(manufacturer);

		MapWithUdt mapWithUdt = new MapWithUdt();
		mapWithUdt.map = Collections.singletonMap("key", new Manufacturer("name", "display"));

		Map<CqlIdentifier, Object> sink = new LinkedHashMap<>();
		mappingCassandraConverter.write(mapWithUdt, sink);

		Map<String, UdtValue> map = (Map) sink.get(CqlIdentifier.fromCql("map"));
		assertThat(map.get("key")).isInstanceOf(UdtValue.class);
	}

	@UserDefinedType
	@Data
	@AllArgsConstructor
	private static class Manufacturer {

		String name;
		@ReadOnlyProperty String displayName;
	}

	@UserDefinedType
	@Data
	@AllArgsConstructor
	private static class Currency {
		String currency;
	}

	@Data
	@AllArgsConstructor
	private static class Supplier {
		Map<Manufacturer, List<Currency>> acceptedCurrencies;
	}

	@Data
	static class OuterWithNullableEmbeddedType {

		@Id String id;

		WithNullableEmbeddedType udtValue;
	}

	@Data
	static class OuterWithPrefixedNullableEmbeddedType {

		@Id String id;

		WithPrefixedNullableEmbeddedType udtValue;
	}

	@UserDefinedType
	@Data
	static class WithNullableEmbeddedType {

		String value;

		@Embedded.Nullable EmbeddedWithSimpleTypes nested;
	}

	@UserDefinedType
	@Data
	static class WithPrefixedNullableEmbeddedType {

		String value;

		@Embedded.Nullable(prefix = "prefix") EmbeddedWithSimpleTypes nested;
	}

	@UserDefinedType
	@Data
	static class WithEmptyEmbeddedType {

		String value;

		@Embedded.Empty EmbeddedWithSimpleTypes nested;
	}

	@Data
	static class EmbeddedWithSimpleTypes {

		String firstname;
		Integer age;

		public String getFirstname() {
			return firstname;
		}

		public Integer getAge() {
			return age;
		}
	}

	static class MapWithUdt {

		@Id String id;

		@CassandraType(type = CassandraType.Name.MAP, userTypeName = "udt", typeArguments = { CassandraType.Name.TEXT,
				CassandraType.Name.UDT }) private Map<String, @Frozen Manufacturer> map;
	}

}
