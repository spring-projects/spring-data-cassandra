/*
 * Copyright 2018-present the original author or authors.
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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.data.cassandra.core.cql.util.StatementBuilder;
import org.springframework.data.cassandra.core.mapping.*;
import org.springframework.data.cassandra.support.UserDefinedTypeBuilder;
import org.springframework.data.cassandra.test.util.RowMockUtil;
import org.springframework.util.ObjectUtils;

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

	@Mock UserTypeResolver typeResolver;

	private com.datastax.oss.driver.api.core.type.UserDefinedType manufacturer = UserDefinedTypeBuilder
			.forName("manufacturer").withField("name", DataTypes.TEXT).withField("displayname", DataTypes.TEXT).build();

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
		mappingContext.setUserTypeResolver(typeResolver);

		CassandraCustomConversions cassandraCustomConversions = new CassandraCustomConversions(
				Arrays.asList(new UDTToCurrencyConverter(), new CurrencyToUDTConverter(typeResolver)));
		mappingContext.setSimpleTypeHolder(cassandraCustomConversions.getSimpleTypeHolder());

		converter = new MappingCassandraConverter(mappingContext);
		converter.setCustomConversions(cassandraCustomConversions);
		converter.afterPropertiesSet();

		when(typeResolver.resolveType(CqlIdentifier.fromCql("address"))).thenReturn(address);
		when(typeResolver.resolveType(CqlIdentifier.fromCql("engine"))).thenReturn(engine);
		when(typeResolver.resolveType(CqlIdentifier.fromCql("manufacturer"))).thenReturn(manufacturer);
		when(typeResolver.resolveType(CqlIdentifier.fromCql("currency"))).thenReturn(currency);
		when(typeResolver.resolveType(CqlIdentifier.fromCql("withnullableembeddedtype")))
				.thenReturn(withnullableembeddedtype);
		when(typeResolver.resolveType(CqlIdentifier.fromCql("withprefixednullableembeddedtype")))
				.thenReturn(withprefixednullableembeddedtype);
	}

	@Test // DATACASS-172
	void shouldWriteMappedUdt() {

		AddressUserType addressUserType = prepareAddressUserType();

		AddressBook addressBook = new AddressBook();
		addressBook.setId("1");
		addressBook.setCurrentaddress(addressUserType);

		SimpleStatement statement = new StatementFactory(converter).insert(addressBook, WriteOptions.empty())
				.build(StatementBuilder.ParameterHandling.INLINE);

		assertThat(statement.getQuery()).isEqualTo("INSERT INTO addressbook (id,currentaddress) "
				+ "VALUES ('1',{zip:'69469',city:'Weinheim',streetlines:['Heckenpfad','14']})");
	}

	@Test // DATACASS-172
	void shouldWriteMappedUdtCollection() {

		AddressUserType addressUserType = prepareAddressUserType();

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

		assertThat(statement.getQuery()).isEqualTo("INSERT INTO addressbook (id,alternate) "
				+ "VALUES ('1',{zip:'69469',city:'Weinheim',streetlines:['Heckenpfad','14']})");
	}

	@Test // DATACASS-172
	void shouldWriteUdtPk() {

		AddressUserType addressUserType = prepareAddressUserType();

		WithMappedUdtId withUdtId = new WithMappedUdtId();
		withUdtId.setId(addressUserType);

		SimpleStatement statement = new StatementFactory(converter).insert(withUdtId, WriteOptions.empty())
				.build(StatementBuilder.ParameterHandling.INLINE);

		assertThat(statement.getQuery()).isEqualTo(
				"INSERT INTO withmappedudtid (id) " + "VALUES ({zip:'69469',city:'Weinheim',streetlines:['Heckenpfad','14']})");
	}

	@Test // #1137
	void shouldWriteCompositeUdtPk() {

		AddressUserType addressUserType = prepareAddressUserType();

		WithCompositePrimaryKey withUdt = new WithCompositePrimaryKey();
		withUdt.addressUserType = addressUserType;
		withUdt.id = "foo";

		SimpleStatement statement = new StatementFactory(converter).insert(withUdt, WriteOptions.empty())
				.build(StatementBuilder.ParameterHandling.INLINE);

		assertThat(statement.getQuery()).isEqualTo("INSERT INTO withcompositeprimarykey (id,addressusertype) "
				+ "VALUES ('foo',{zip:'69469',city:'Weinheim',streetlines:['Heckenpfad','14']})");
	}

	@Test // GH-1473
	void shouldWriteMapCorrectly() {

		Manufacturer manufacturer = new Manufacturer("foo", "bar");
		AddressUserType addressUserType = prepareAddressUserType();

		Map<Manufacturer, AddressUserType> value = Map.of(manufacturer, addressUserType);
		Map<?, ?> writeValue = (Map<?, ?>) converter.convertToColumnType(value);
		Map.Entry<?, ?> entry = writeValue.entrySet().iterator().next();

		assertThat(entry.getKey()).isInstanceOf(UdtValue.class);
		assertThat(entry.getValue()).isInstanceOf(UdtValue.class);
	}

	@Test // GH-1473
	void shouldWriteSetCorrectly() {

		AddressUserType addressUserType = prepareAddressUserType();

		Set<AddressUserType> value = Set.of(addressUserType);
		Set<?> writeValue = (Set<?>) converter.convertToColumnType(value);

		assertThat(writeValue.iterator().next()).isInstanceOf(UdtValue.class);
	}

	private static AddressUserType prepareAddressUserType() {

		AddressUserType addressUserType = new AddressUserType();
		addressUserType.setZip("69469");
		addressUserType.setCity("Weinheim");
		addressUserType.setStreetLines(Arrays.asList("Heckenpfad", "14"));

		return addressUserType;
	}

	@Test // #1137
	void shouldWriteCompositeUdtPkClass() {

		WithCompositePrimaryKeyClassWithUdt object = prepareCompositePrimaryKeyClassWithUdt();

		SimpleStatement statement = new StatementFactory(converter).insert(object, WriteOptions.empty())
				.build(StatementBuilder.ParameterHandling.INLINE);

		assertThat(statement.getQuery())
				.isEqualTo("INSERT INTO withcompositeprimarykeyclasswithudt (id,addressusertype,currency) "
						+ "VALUES ('foo',{zip:'69469',city:'Weinheim',streetlines:['Heckenpfad','14']},{currency:'EUR'})");
	}

	@Test // #1137
	void shouldWriteCompositeUdtPkClassToWhere() {

		WithCompositePrimaryKeyClassWithUdt object = prepareCompositePrimaryKeyClassWithUdt();

		Where where = new Where();
		converter.write(object, where);

		assertThat((UdtValue) where.get(CqlIdentifier.fromCql("currency"))) //
				.extracting(UdtValue::getFormattedContents) //
				.isEqualTo("{currency:'EUR'}");
	}

	private static WithCompositePrimaryKeyClassWithUdt prepareCompositePrimaryKeyClassWithUdt() {

		AddressUserType addressUserType = prepareAddressUserType();

		CompositePrimaryKeyClassWithUdt withUdt = new CompositePrimaryKeyClassWithUdt();
		withUdt.addressUserType = addressUserType;
		withUdt.id = "foo";
		withUdt.currency = new Currency("EUR");

		WithCompositePrimaryKeyClassWithUdt object = new WithCompositePrimaryKeyClassWithUdt();
		object.id = withUdt;
		return object;
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

		OuterWithPrefixedNullableEmbeddedType target = converter.read(OuterWithPrefixedNullableEmbeddedType.class, rowMock);
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
				.isEqualTo("INSERT INTO car (id,engine) VALUES ('1',{manufacturer:{name:'a good one',displayname:NULL}})");
	}

	@Test // #1098
	void shouldWriteMapWithTypeHintToUdtValue() {

		when(typeResolver.resolveType(CqlIdentifier.fromCql("udt"))).thenReturn(manufacturer);

		MapWithUdt mapWithUdt = new MapWithUdt();
		mapWithUdt.map = Collections.singletonMap("key", new Manufacturer("name", "display"));

		Map<CqlIdentifier, Object> sink = new LinkedHashMap<>();
		converter.write(mapWithUdt, sink);

		Map<String, UdtValue> map = (Map) sink.get(CqlIdentifier.fromCql("map"));
		assertThat(map.get("key")).isInstanceOf(UdtValue.class);
	}

	@Table
	private static class Car {

		@Id String id;
		Engine engine;

		public Car(String id, Engine engine) {
			this.id = id;
			this.engine = engine;
		}

		public String getId() {
			return this.id;
		}

		public Engine getEngine() {
			return this.engine;
		}
	}

	@UserDefinedType
	private static class Engine {
		Manufacturer manufacturer;

		public Engine(Manufacturer manufacturer) {
			this.manufacturer = manufacturer;
		}

		public Manufacturer getManufacturer() {
			return this.manufacturer;
		}
	}

	@UserDefinedType
	private static class Manufacturer {
		String name;
		@ReadOnlyProperty String displayName;

		public Manufacturer(String name, String displayName) {
			this.name = name;
			this.displayName = displayName;
		}

		public String getName() {
			return this.name;
		}

		public String getDisplayName() {
			return this.displayName;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setDisplayName(String displayName) {
			this.displayName = displayName;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;

			Manufacturer that = (Manufacturer) o;

			if (!ObjectUtils.nullSafeEquals(name, that.name)) {
				return false;
			}
			return ObjectUtils.nullSafeEquals(displayName, that.displayName);
		}

		@Override
		public int hashCode() {
			int result = ObjectUtils.nullSafeHashCode(name);
			result = 31 * result + ObjectUtils.nullSafeHashCode(displayName);
			return result;
		}
	}

	private static class Currency {
		String currency;

		public Currency(String currency) {
			this.currency = currency;
		}

		public String getCurrency() {
			return this.currency;
		}

		public void setCurrency(String currency) {
			this.currency = currency;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;

			Currency currency1 = (Currency) o;

			return ObjectUtils.nullSafeEquals(currency, currency1.currency);
		}

		@Override
		public int hashCode() {
			return ObjectUtils.nullSafeHashCode(currency);
		}
	}

	private static class Supplier {
		Map<Manufacturer, List<Currency>> acceptedCurrencies;

		public Supplier(Map<Manufacturer, List<Currency>> acceptedCurrencies) {
			this.acceptedCurrencies = acceptedCurrencies;
		}

		public Map<Manufacturer, List<Currency>> getAcceptedCurrencies() {
			return this.acceptedCurrencies;
		}

		public void setAcceptedCurrencies(Map<Manufacturer, List<Currency>> acceptedCurrencies) {
			this.acceptedCurrencies = acceptedCurrencies;
		}
	}

	static class OuterWithNullableEmbeddedType {

		@Id String id;

		WithNullableEmbeddedType udtValue;

		public String getId() {
			return this.id;
		}

		public WithNullableEmbeddedType getUdtValue() {
			return this.udtValue;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setUdtValue(WithNullableEmbeddedType udtValue) {
			this.udtValue = udtValue;
		}
	}

	static class OuterWithPrefixedNullableEmbeddedType {

		@Id String id;

		WithPrefixedNullableEmbeddedType udtValue;

		public String getId() {
			return this.id;
		}

		public WithPrefixedNullableEmbeddedType getUdtValue() {
			return this.udtValue;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setUdtValue(WithPrefixedNullableEmbeddedType udtValue) {
			this.udtValue = udtValue;
		}
	}

	@UserDefinedType
	static class WithNullableEmbeddedType {

		String value;

		@Embedded.Nullable EmbeddedWithSimpleTypes nested;

		public String getValue() {
			return this.value;
		}

		public EmbeddedWithSimpleTypes getNested() {
			return this.nested;
		}

		public void setValue(String value) {
			this.value = value;
		}

		public void setNested(EmbeddedWithSimpleTypes nested) {
			this.nested = nested;
		}
	}

	@UserDefinedType
	static class WithPrefixedNullableEmbeddedType {

		String value;

		@Embedded.Nullable(prefix = "prefix") EmbeddedWithSimpleTypes nested;

		public String getValue() {
			return this.value;
		}

		public EmbeddedWithSimpleTypes getNested() {
			return this.nested;
		}

		public void setValue(String value) {
			this.value = value;
		}

		public void setNested(EmbeddedWithSimpleTypes nested) {
			this.nested = nested;
		}
	}

	@UserDefinedType
	static class WithEmptyEmbeddedType {

		String value;

		@Embedded.Empty EmbeddedWithSimpleTypes nested;

		public String getValue() {
			return this.value;
		}

		public EmbeddedWithSimpleTypes getNested() {
			return this.nested;
		}

		public void setValue(String value) {
			this.value = value;
		}

		public void setNested(EmbeddedWithSimpleTypes nested) {
			this.nested = nested;
		}
	}

	static class EmbeddedWithSimpleTypes {

		String firstname;
		Integer age;

		public String getFirstname() {
			return firstname;
		}

		public Integer getAge() {
			return age;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		public void setAge(Integer age) {
			this.age = age;
		}
	}

	@Table
	public static class AddressBook {

		@Id private String id;

		private AddressUserType currentaddress;
		private List<AddressUserType> previousaddresses;
		private UdtValue alternate;

		public String getId() {
			return this.id;
		}

		public AddressUserType getCurrentaddress() {
			return this.currentaddress;
		}

		public List<AddressUserType> getPreviousaddresses() {
			return this.previousaddresses;
		}

		public UdtValue getAlternate() {
			return this.alternate;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setCurrentaddress(AddressUserType currentaddress) {
			this.currentaddress = currentaddress;
		}

		public void setPreviousaddresses(List<AddressUserType> previousaddresses) {
			this.previousaddresses = previousaddresses;
		}

		public void setAlternate(UdtValue alternate) {
			this.alternate = alternate;
		}
	}

	@Table
	public static class WithUdtId {
		@Id private UdtValue id;

		public UdtValue getId() {
			return this.id;
		}

		public void setId(UdtValue id) {
			this.id = id;
		}
	}

	@Table
	public static class WithMappedUdtId {
		@Id private AddressUserType id;

		public AddressUserType getId() {
			return this.id;
		}

		public void setId(AddressUserType id) {
			this.id = id;
		}
	}

	@UserDefinedType("address")
	public static class AddressUserType {

		String zip;
		String city;

		List<String> streetLines;

		public String getZip() {
			return this.zip;
		}

		public String getCity() {
			return this.city;
		}

		public List<String> getStreetLines() {
			return this.streetLines;
		}

		public void setZip(String zip) {
			this.zip = zip;
		}

		public void setCity(String city) {
			this.city = city;
		}

		public void setStreetLines(List<String> streetLines) {
			this.streetLines = streetLines;
		}
	}

	@Table
	private static class Bank {

		@Id String id;
		Currency currency;
		List<Currency> otherCurrencies;

		public Bank(String id, Currency currency, List<Currency> otherCurrencies) {
			this.id = id;
			this.currency = currency;
			this.otherCurrencies = otherCurrencies;
		}

		public String getId() {
			return this.id;
		}

		public Currency getCurrency() {
			return this.currency;
		}

		public List<Currency> getOtherCurrencies() {
			return this.otherCurrencies;
		}
	}

	@Table
	public static class Money {
		@Id private Currency currency;

		public Currency getCurrency() {
			return this.currency;
		}

		public void setCurrency(Currency currency) {
			this.currency = currency;
		}
	}

	@Table
	public static class WithCompositePrimaryKey {
		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) String id;
		@PrimaryKeyColumn(ordinal = 1) AddressUserType addressUserType;

		public String getId() {
			return this.id;
		}

		public AddressUserType getAddressUserType() {
			return this.addressUserType;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setAddressUserType(AddressUserType addressUserType) {
			this.addressUserType = addressUserType;
		}
	}

	@Table
	public static class WithCompositePrimaryKeyClassWithUdt {
		@PrimaryKey CompositePrimaryKeyClassWithUdt id;

		public CompositePrimaryKeyClassWithUdt getId() {
			return this.id;
		}

		public void setId(CompositePrimaryKeyClassWithUdt id) {
			this.id = id;
		}
	}

	@PrimaryKeyClass
	public static class CompositePrimaryKeyClassWithUdt {
		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) String id;
		@PrimaryKeyColumn(ordinal = 1) AddressUserType addressUserType;
		@PrimaryKeyColumn(ordinal = 2) Currency currency;

		public String getId() {
			return this.id;
		}

		public AddressUserType getAddressUserType() {
			return this.addressUserType;
		}

		public Currency getCurrency() {
			return this.currency;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setAddressUserType(AddressUserType addressUserType) {
			this.addressUserType = addressUserType;
		}

		public void setCurrency(Currency currency) {
			this.currency = currency;
		}
	}

	@Table
	public static class MoneyTransfer {

		@Id String id;

		private Currency currency;

		public MoneyTransfer(String id, Currency currency) {
			this.id = id;
			this.currency = currency;
		}

		public String getId() {
			return this.id;
		}

		public Currency getCurrency() {
			return this.currency;
		}
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
