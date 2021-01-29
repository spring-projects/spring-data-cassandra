/*
 * Copyright 2017-2021 the original author or authors.
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

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Currency;
import java.util.LinkedHashSet;
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

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.Element;
import org.springframework.data.cassandra.core.mapping.Embedded;
import org.springframework.data.cassandra.core.mapping.Indexed;
import org.springframework.data.cassandra.core.mapping.Tuple;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;
import org.springframework.data.cassandra.core.mapping.UserTypeResolver;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.data.cassandra.support.UserDefinedTypeBuilder;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataTypes;

/**
 * Unit tests for {@link UpdateMapper}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Marko Janković
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class UpdateMapperUnitTests {

	private CassandraMappingContext mappingContext = new CassandraMappingContext();

	private CassandraPersistentEntity<?> persistentEntity;

	private Currency currencyEUR = Currency.getInstance("EUR");
	private Currency currencyUSD = Currency.getInstance("USD");

	private MappingCassandraConverter cassandraConverter;

	private UpdateMapper updateMapper;

	private com.datastax.oss.driver.api.core.type.UserDefinedType manufacturer = UserDefinedTypeBuilder
			.forName("manufacturer")
			.withField("name", DataTypes.TEXT).build();

	@Mock UserTypeResolver userTypeResolver;

	@BeforeEach
	void before() {

		CassandraCustomConversions customConversions = new CassandraCustomConversions(
				Collections.singletonList(CurrencyConverter.INSTANCE));

		mappingContext.setCustomConversions(customConversions);
		mappingContext.setUserTypeResolver(userTypeResolver);

		cassandraConverter = new MappingCassandraConverter(mappingContext);
		cassandraConverter.setCustomConversions(customConversions);
		cassandraConverter.afterPropertiesSet();

		updateMapper = new UpdateMapper(cassandraConverter);

		persistentEntity = mappingContext.getRequiredPersistentEntity(Person.class);

		when(userTypeResolver.resolveType(CqlIdentifier.fromCql("manufacturer"))).thenReturn(manufacturer);
	}

	@Test // DATACASS-343
	void shouldCreateSimpleUpdate() {

		Update update = updateMapper.getMappedObject(Update.empty().set("firstName", "foo"), persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("first_name = 'foo'");
	}

	@Test // DATACASS-487
	void shouldReplaceUdtMap() {

		Manufacturer manufacturer = new Manufacturer("foobar");

		Map<Manufacturer, Currency> map = Collections.singletonMap(manufacturer, currencyEUR);

		Update update = Update.empty().set("manufacturers", map);

		Update mappedUpdate = updateMapper.getMappedObject(update, persistentEntity);

		assertThat(mappedUpdate.getUpdateOperations()).hasSize(1);
		assertThat(mappedUpdate).hasToString("manufacturers = {{name:'foobar'}:'Euro'}");
	}

	@Test // DATACASS-343
	void shouldCreateSetAtIndexUpdate() {

		Update update = updateMapper.getMappedObject(Update.empty().set("list").atIndex(10).to(currencyEUR),
				persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("list[10] = 'Euro'");
	}

	@Test // DATACASS-829
	void shouldCreateSetAtUdtIndexUpdate() {

		Update update = updateMapper.getMappedObject(
				Update.empty().set("manufacturerList").atIndex(10).to(new Manufacturer("foo")), persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("manufacturerlist[10] = {name:'foo'}");
	}

	@Test // DATACASS-343
	void shouldCreateSetAtKeyUpdate() {

		Update update = updateMapper.getMappedObject(Update.empty().set("map").atKey("baz").to(currencyEUR),
				persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("map['baz'] = 'Euro'");
	}

	@Test // DATACASS-487
	void shouldCreateSetAtUdtKeyUpdate() {

		Manufacturer manufacturer = new Manufacturer("foobar");

		Update update = updateMapper
				.getMappedObject(Update.empty().set("manufacturers").atKey(manufacturer).to(currencyEUR),
				persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("manufacturers[{name:'foobar'}] = 'Euro'");
	}

	@Test // DATACASS-343
	void shouldAddToMap() {

		Update update = updateMapper.getMappedObject(Update.empty().addTo("map").entry("foo", currencyEUR),
				persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("map = map + {'foo':'Euro'}");
	}

	@Test // #1007
	void shouldRemoveFromMap() {

		Update update = updateMapper.getMappedObject(Update.empty().removeFrom("map").value("foo"), persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("map = map - {'foo'}");
	}

	@Test // DATACASS-487
	void shouldAddUdtToMap() {

		Manufacturer manufacturer = new Manufacturer("foobar");

		Update update = Update.empty().addTo("manufacturers").entry(manufacturer, currencyEUR);

		Update mappedUpdate = updateMapper.getMappedObject(update, persistentEntity);

		assertThat(mappedUpdate.getUpdateOperations()).hasSize(1);
		assertThat(mappedUpdate).hasToString("manufacturers = manufacturers + {{name:'foobar'}:'Euro'}");
	}

	@Test // DATACASS-343
	void shouldPrependAllToList() {

		Update update = updateMapper.getMappedObject(Update.empty().addTo("list").prependAll("foo", currencyEUR),
				persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("list = ['foo','Euro'] + list");
	}

	@Test // DATACASS-343
	void shouldAppendAllToList() {

		Update update = updateMapper.getMappedObject(Update.empty().addTo("list").appendAll("foo", currencyEUR),
				persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("list = list + ['foo','Euro']");
	}

	@Test // DATACASS-343
	void shouldRemoveFromList() {

		Update update = updateMapper.getMappedObject(Update.empty().remove("list", currencyEUR), persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("list = list - ['Euro']");
	}

	@Test // DATACASS-343
	void shouldClearList() {

		Update update = updateMapper.getMappedObject(Update.empty().clear("list"), persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("list = []");
	}

	@Test // DATACASS-770
	void shouldPrependAllToSet() {

		Update update = updateMapper.getMappedObject(Update.empty().addTo("set").prependAll(currencyUSD, currencyEUR),
				persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("set_col = {'US Dollar','Euro'} + set_col");
	}

	@Test // DATACASS-770
	void shouldAppendAllToSet() {

		Update update = updateMapper.getMappedObject(Update.empty().addTo("set").appendAll(currencyUSD, currencyEUR),
				persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("set_col = set_col + {'US Dollar','Euro'}");
	}

	@Test // DATACASS-770
	void shouldPrependAllToSetViaColumnNameCollectionOfElements() {

		Update update = updateMapper.getMappedObject(
				Update.empty().addTo("set_col").prependAll(new LinkedHashSet<>(Arrays.asList(currencyUSD, currencyEUR))),
				persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("set_col = {'US Dollar','Euro'} + set_col");
	}

	@Test // DATACASS-770
	void shouldAppendAllToSetViaColumnNameCollectionOfElements() {

		Update update = updateMapper.getMappedObject(
				Update.empty().addTo("set_col").appendAll(new LinkedHashSet<>(Arrays.asList(currencyUSD, currencyEUR))),
				persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("set_col = set_col + {'US Dollar','Euro'}");
	}

	@Test // DATACASS-770
	void shouldAppendToSet() {

		Update update = updateMapper.getMappedObject(Update.empty().addTo("set").append(currencyEUR),
				persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("set_col = set_col + {'Euro'}");
	}

	@Test // DATACASS-770
	void shouldPrependToSet() {

		Update update = updateMapper.getMappedObject(Update.empty().addTo("set").prepend(currencyEUR),
				persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("set_col = {'Euro'} + set_col");
	}

	@Test // DATACASS-770
	void shouldRemoveFromSet() {

		Update update = updateMapper.getMappedObject(Update.empty().remove("set", currencyEUR), persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("set_col = set_col - {'Euro'}");
	}

	@Test // DATACASS-343
	void shouldClearSet() {

		Update update = updateMapper.getMappedObject(Update.empty().clear("set"), persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("set_col = {}");
	}

	@Test // DATACASS-343
	void shouldCreateIncrementUpdate() {

		Update update = updateMapper.getMappedObject(Update.empty().increment("number"), persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("number = number + 1");
	}

	@Test // DATACASS-343
	void shouldCreateDecrementUpdate() {

		Update update = updateMapper.getMappedObject(Update.empty().decrement("number"), persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("number = number - 1");
	}

	@Test // DATACASS-523
	void shouldMapTuple() {

		Update update = this.updateMapper.getMappedObject(Update.empty().set("tuple", new MappedTuple("foo")),
				this.persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("tuple = ('foo')");
	}

	@Test // DATACASS-302, DATACASS-694
	void shouldMapTime() {

		Update update = this.updateMapper.getMappedObject(Update.empty().set("localTime", LocalTime.of(1, 2, 3)),
				this.persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).startsWith("localtime = '01:02:03.");
	}

	@Test // DATACASS-523
	void referencingTupleElementsInQueryShouldFail() {
		assertThatIllegalArgumentException().isThrownBy(
				() -> this.updateMapper.getMappedObject(Update.empty().set("tuple.zip", "bar"), this.persistentEntity));
	}

	@Test // DATACASS-167
	void shouldMapEmbeddedEntity() {

		Update update = this.updateMapper.getMappedObject(Update.empty().set("nested.firstname", "spring"),
				mappingContext.getRequiredPersistentEntity(WithNullableEmbeddedType.class));

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).startsWith("firstname = 'spring'");
	}

	@Test // DATACASS-167
	void shouldMapPrefixedEmbeddedEntity() {

		Update update = this.updateMapper.getMappedObject(Update.empty().set("nested.firstname", "spring"),
				mappingContext.getRequiredPersistentEntity(WithPrefixedNullableEmbeddedType.class));

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).startsWith("prefixfirstname = 'spring'");
	}

	@SuppressWarnings("unused")
	static class Person {

		@Id String id;

		Currency currency;

		Integer number;

		List<Currency> list;

		LocalTime localTime;

		Map<String, Currency> map;
		Map<Manufacturer, Currency> manufacturers;

		List<Manufacturer> manufacturerList;

		MappedTuple tuple;

		@Column("set_col") Set<Currency> set;

		@Column("first_name") String firstName;

	}

	@Tuple
	@AllArgsConstructor
	static class MappedTuple {
		@Element(0) String zip;
	}

	@Data
	@UserDefinedType
	@AllArgsConstructor
	static class Manufacturer {
		String name;
	}

	@Data
	static class WithNullableEmbeddedType {

		@Id String id;

		@Embedded.Nullable EmbeddedWithSimpleTypes nested;
	}

	@Data
	static class WithPrefixedNullableEmbeddedType {

		@Id String id;

		// @Indexed -> index for all properties of nested
		@Embedded.Nullable(prefix = "prefix") EmbeddedWithSimpleTypes nested;
	}

	@Data
	static class EmbeddedWithSimpleTypes {

		@Indexed String firstname;
		Integer age;
	}

}
