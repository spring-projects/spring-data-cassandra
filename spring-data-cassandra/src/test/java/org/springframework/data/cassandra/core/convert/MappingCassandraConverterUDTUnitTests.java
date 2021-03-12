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
import lombok.Getter;

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

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.ReadOnlyProperty;
import org.springframework.data.cassandra.core.StatementFactory;
import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.data.cassandra.core.cql.util.StatementBuilder;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Embedded;
import org.springframework.data.cassandra.core.mapping.Frozen;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;
import org.springframework.data.cassandra.core.mapping.UserTypeResolver;
import org.springframework.data.cassandra.support.UserDefinedTypeBuilder;
import org.springframework.data.cassandra.test.util.RowMockUtil;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
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

	private com.datastax.oss.driver.api.core.type.UserDefinedType engine = UserDefinedTypeBuilder.forName("engine")
			.withField("manufacturer", manufacturer).build();

	private com.datastax.oss.driver.api.core.type.UserDefinedType currency = UserDefinedTypeBuilder.forName("mycurrency")
			.withField("currency", DataTypes.TEXT).build();

	private com.datastax.oss.driver.api.core.type.UserDefinedType withnullableembeddedtype = UserDefinedTypeBuilder
			.forName("withnullableembeddedtype").withField("value", DataTypes.TEXT).withField("firstname", DataTypes.TEXT)
			.withField("age", DataTypes.INT).build();

	private com.datastax.oss.driver.api.core.type.UserDefinedType withprefixednullableembeddedtype = UserDefinedTypeBuilder
			.forName("withnullableembeddedtype").withField("value", DataTypes.TEXT)
			.withField("prefixfirstname", DataTypes.TEXT).withField("prefixage", DataTypes.INT).build();

	private com.datastax.oss.driver.api.core.type.UserDefinedType address = UserDefinedTypeBuilder.forName("address")
			.withField("zip", DataTypes.TEXT).withField("city", DataTypes.TEXT)
			.withField("streetLines", DataTypes.listOf(DataTypes.TEXT)).build();

	private Row rowMock;

	private CassandraMappingContext mappingContext;
	private MappingCassandraConverter converter;

	@BeforeEach
	void setUp() {

		mappingContext = new CassandraMappingContext();
		mappingContext.setUserTypeResolver(userTypeResolver);

		CassandraCustomConversions cassandraCustomConversions = new CassandraCustomConversions(
				Arrays.asList(new UDTToCurrencyConverter(), new CurrencyToUDTConverter(userTypeResolver)));
		mappingContext.setSimpleTypeHolder(cassandraCustomConversions.getSimpleTypeHolder());

		converter = new MappingCassandraConverter(mappingContext);
		converter.setCustomConversions(cassandraCustomConversions);
		converter.afterPropertiesSet();

		when(userTypeResolver.resolveType(CqlIdentifier.fromCql("address"))).thenReturn(address);
		when(userTypeResolver.resolveType(CqlIdentifier.fromCql("engine"))).thenReturn(engine);
		when(userTypeResolver.resolveType(CqlIdentifier.fromCql("manufacturer"))).thenReturn(manufacturer);
		when(userTypeResolver.resolveType(CqlIdentifier.fromCql("currency"))).thenReturn(currency);
		when(userTypeResolver.resolveType(CqlIdentifier.fromCql("withnullableembeddedtype")))
				.thenReturn(withnullableembeddedtype);
		when(userTypeResolver.resolveType(CqlIdentifier.fromCql("withprefixednullableembeddedtype")))
				.thenReturn(withprefixednullableembeddedtype);
	}

	@Test // DATACASS-172
	void shouldWriteMappedUdt() {

		AddressUserType addressUserType = new AddressUserType();
		addressUserType.setZip("69469");
		addressUserType.setCity("Weinheim");
		addressUserType.setStreetLines(Arrays.asList("Heckenpfad", "14"));

		AddressBook addressBook = new AddressBook();
		addressBook.setId("1");
		addressBook.setCurrentaddress(addressUserType);

		SimpleStatement statement = new StatementFactory(converter).insert(addressBook, WriteOptions.empty())
				.build(StatementBuilder.ParameterHandling.INLINE);

		assertThat(statement.getQuery()).isEqualTo("INSERT INTO addressbook (currentaddress,id) "
				+ "VALUES ({zip:'69469',city:'Weinheim',streetlines:['Heckenpfad','14']},'1')");
	}

	@Test // DATACASS-172
	void shouldWriteMappedUdtCollection() {

		AddressUserType addressUserType = new AddressUserType();
		addressUserType.setZip("69469");
		addressUserType.setCity("Weinheim");
		addressUserType.setStreetLines(Arrays.asList("Heckenpfad", "14"));

		AddressBook addressBook = new AddressBook();
		addressBook.setId("1");
		addressBook.setPreviousaddresses(Collections.singletonList(addressUserType));

		SimpleStatement statement = new StatementFactory(converter).insert(addressBook, WriteOptions.empty())
				.build(StatementBuilder.ParameterHandling.INLINE);

		assertThat(statement.getQuery()).isEqualTo("INSERT INTO addressbook (id,previousaddresses) "
				+ "VALUES ('1',[{zip:'69469',city:'Weinheim',streetlines:['Heckenpfad','14']}])");
	}

	@Test // DATACASS-172
	void shouldWriteUdt() {

		CassandraPersistentEntity<?> persistentEntity = converter.getMappingContext()
				.getRequiredPersistentEntity(AddressUserType.class);
		com.datastax.oss.driver.api.core.type.UserDefinedType udtType = (com.datastax.oss.driver.api.core.type.UserDefinedType) converter
				.getColumnTypeResolver().resolve(persistentEntity.getTypeInformation()).getDataType();
		UdtValue udtValue = udtType.newValue();
		udtValue.setString("zip", "69469");
		udtValue.setString("city", "Weinheim");
		udtValue.setList("streetlines", Arrays.asList("Heckenpfad", "14"), String.class);

		AddressBook addressBook = new AddressBook();
		addressBook.setId("1");
		addressBook.setAlternate(udtValue);

		SimpleStatement statement = new StatementFactory(converter).insert(addressBook, WriteOptions.empty())
				.build(StatementBuilder.ParameterHandling.INLINE);

		assertThat(statement.getQuery()).isEqualTo("INSERT INTO addressbook (alternate,id) "
				+ "VALUES ({zip:'69469',city:'Weinheim',streetlines:['Heckenpfad','14']},'1')");
	}

	@Test // DATACASS-172
	void shouldWriteUdtPk() {

		AddressUserType addressUserType = new AddressUserType();
		addressUserType.setZip("69469");
		addressUserType.setCity("Weinheim");
		addressUserType.setStreetLines(Arrays.asList("Heckenpfad", "14"));

		WithMappedUdtId withUdtId = new WithMappedUdtId();
		withUdtId.setId(addressUserType);

		SimpleStatement statement = new StatementFactory(converter).insert(withUdtId, WriteOptions.empty())
				.build(StatementBuilder.ParameterHandling.INLINE);

		assertThat(statement.getQuery()).isEqualTo(
				"INSERT INTO withmappedudtid (id) " + "VALUES ({zip:'69469',city:'Weinheim',streetlines:['Heckenpfad','14']})");
	}

	@Test // DATACASS-172
	void shouldWriteMappedUdtPk() {

		CassandraPersistentEntity<?> persistentEntity = converter.getMappingContext()
				.getRequiredPersistentEntity(AddressUserType.class);

		com.datastax.oss.driver.api.core.type.UserDefinedType udtType = (com.datastax.oss.driver.api.core.type.UserDefinedType) converter
				.getColumnTypeResolver().resolve(persistentEntity.getTypeInformation()).getDataType();

		UdtValue udtValue = udtType.newValue();
		udtValue.setString("zip", "69469");
		udtValue.setString("city", "Weinheim");
		udtValue.setList("streetlines", Arrays.asList("Heckenpfad", "14"), String.class);

		WithUdtId withUdtId = new WithUdtId();
		withUdtId.setId(udtValue);

		SimpleStatement statement = new StatementFactory(converter).insert(withUdtId, WriteOptions.empty())
				.build(StatementBuilder.ParameterHandling.INLINE);

		assertThat(statement.getQuery()).isEqualTo(
				"INSERT INTO withudtid (id) " + "VALUES ({zip:'69469',city:'Weinheim',streetlines:['Heckenpfad','14']})");
	}

	@Test // DATACASS-172, DATACASS-400
	void shouldWriteUdtWithCustomConversion() {

		Bank bank = new Bank(null, new Currency("EUR"), null);

		SimpleStatement statement = new StatementFactory(converter).insert(bank, WriteOptions.empty())
				.build(StatementBuilder.ParameterHandling.INLINE);

		assertThat(statement.getQuery()).isEqualTo("INSERT INTO bank (currency) VALUES ({currency:'EUR'})");
	}

	@Test // DATACASS-172
	void shouldWriteUdtWhereWherePrimaryKeyWithCustomConversion() {

		Money money = new Money();
		money.setCurrency(new Currency("EUR"));

		Where where = new Where();
		converter.write(money, where);

		assertThat((UdtValue) where.get(CqlIdentifier.fromCql("currency"))) //
				.extracting(UdtValue::getFormattedContents) //
				.isEqualTo("{currency:'EUR'}");
	}

	@Test // DATACASS-172, DATACASS-400
	void shouldWriteUdtUpdateAssignmentsWithCustomConversion() {

		MoneyTransfer money = new MoneyTransfer("1", new Currency("EUR"));

		SimpleStatement statement = new StatementFactory(converter).update(money, WriteOptions.empty())
				.build(StatementBuilder.ParameterHandling.INLINE);

		assertThat(statement.getQuery()).isEqualTo("UPDATE moneytransfer SET currency={currency:'EUR'} WHERE id='1'");
	}

	@Test // DATACASS-172, DATACASS-400
	void shouldWriteUdtListWithCustomConversion() {

		Bank bank = new Bank(null, null, Collections.singletonList(new Currency("EUR")));

		SimpleStatement statement = new StatementFactory(converter).insert(bank, WriteOptions.empty())
				.build(StatementBuilder.ParameterHandling.INLINE);

		assertThat(statement.getQuery()).isEqualTo("INSERT INTO bank (othercurrencies) VALUES ([{currency:'EUR'}])");
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

		Supplier supplier = converter.read(Supplier.class, rowMock);

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

		converter.write(supplier, insert);

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

		converter.write(entity, sink);

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

		converter.write(entity, sink);

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

		converter.write(entity, sink);

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

		OuterWithNullableEmbeddedType target = converter.read(OuterWithNullableEmbeddedType.class, rowMock);
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

		OuterWithPrefixedNullableEmbeddedType target = converter
				.read(OuterWithPrefixedNullableEmbeddedType.class, rowMock);
		assertThat(target.getId()).isEqualTo("id-1");
		assertThat(target.udtValue).isNotNull();
		assertThat(target.udtValue.value).isEqualTo("value-string");
		assertThat(target.udtValue.nested.firstname).isEqualTo("fn");
		assertThat(target.udtValue.nested.age).isEqualTo(30);
	}

	@Test // DATACASS-172, DATACASS-400
	void shouldWriteNestedUdt() {

		Engine engine = new Engine(new Manufacturer("a good one", "display name"));

		Car car = new Car("1", engine);

		SimpleStatement statement = new StatementFactory(converter).insert(car, WriteOptions.empty())
				.build(StatementBuilder.ParameterHandling.INLINE);

		assertThat(statement.getQuery())
				.isEqualTo("INSERT INTO car (engine,id) VALUES ({manufacturer:{name:'a good one',displayname:NULL}},'1')");
	}

	@Test // #1098
	void shouldWriteMapWithTypeHintToUdtValue() {

		when(userTypeResolver.resolveType(CqlIdentifier.fromCql("udt"))).thenReturn(manufacturer);

		MapWithUdt mapWithUdt = new MapWithUdt();
		mapWithUdt.map = Collections.singletonMap("key", new Manufacturer("name", "display"));

		Map<CqlIdentifier, Object> sink = new LinkedHashMap<>();
		converter.write(mapWithUdt, sink);

		Map<String, UdtValue> map = (Map) sink.get(CqlIdentifier.fromCql("map"));
		assertThat(map.get("key")).isInstanceOf(UdtValue.class);
	}

	@Table
	@Getter
	@AllArgsConstructor
	private static class Car {

		@Id String id;
		Engine engine;
	}

	@UserDefinedType
	@Getter
	@AllArgsConstructor
	private static class Engine {
		Manufacturer manufacturer;
	}

	@UserDefinedType
	@Data
	@AllArgsConstructor
	private static class Manufacturer {
		String name;
		@ReadOnlyProperty String displayName;
	}

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

	@Data
	@Table
	public static class AddressBook {

		@Id private String id;

		private AddressUserType currentaddress;
		private List<AddressUserType> previousaddresses;
		private UdtValue alternate;
	}

	@Data
	@Table
	public static class WithUdtId {
		@Id private UdtValue id;
	}

	@Data
	@Table
	public static class WithMappedUdtId {
		@Id private AddressUserType id;
	}

	@UserDefinedType("address")
	@Data
	public static class AddressUserType {

		String zip;
		String city;

		List<String> streetLines;
	}

	@Table
	@Getter
	@AllArgsConstructor
	private static class Bank {

		@Id String id;
		Currency currency;
		List<Currency> otherCurrencies;
	}

	@Data
	@Table
	public static class Money {
		@Id private Currency currency;
	}

	@Table
	@AllArgsConstructor
	@Getter
	public static class MoneyTransfer {

		@Id String id;

		private Currency currency;
	}

	private static class UDTToCurrencyConverter implements Converter<UdtValue, Currency> {

		@Override
		public Currency convert(UdtValue source) {
			return new Currency(source.getString("currency"));
		}
	}

	private static class CurrencyToUDTConverter implements Converter<Currency, UdtValue> {

		private final UserTypeResolver userTypeResolver;

		CurrencyToUDTConverter(UserTypeResolver userTypeResolver) {
			this.userTypeResolver = userTypeResolver;
		}

		@Override
		public UdtValue convert(Currency source) {
			com.datastax.oss.driver.api.core.type.UserDefinedType userType = userTypeResolver
					.resolveType(CqlIdentifier.fromCql("currency"));
			UdtValue udtValue = userType.newValue();
			udtValue.setString("currency", source.getCurrency());
			return udtValue;
		}
	}

	static class MapWithUdt {

		@Id String id;

		@CassandraType(type = CassandraType.Name.MAP, userTypeName = "udt", typeArguments = { CassandraType.Name.TEXT,
				CassandraType.Name.UDT }) private Map<String, @Frozen Manufacturer> map;
	}

}
