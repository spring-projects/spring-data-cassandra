/*
 * Copyright 2017-2020 the original author or authors.
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
import java.util.Collections;
import java.util.Currency;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
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
 */
@RunWith(MockitoJUnitRunner.class)
public class UpdateMapperUnitTests {

	CassandraMappingContext mappingContext = new CassandraMappingContext();

	CassandraPersistentEntity<?> persistentEntity;

	Currency currency = Currency.getInstance("EUR");

	MappingCassandraConverter cassandraConverter;

	UpdateMapper updateMapper;

	com.datastax.oss.driver.api.core.type.UserDefinedType manufacturer = UserDefinedTypeBuilder.forName("manufacturer")
			.withField("name", DataTypes.TEXT).build();

	@Mock UserTypeResolver userTypeResolver;

	@Before
	public void before() {

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
	public void shouldCreateSimpleUpdate() {

		Update update = updateMapper.getMappedObject(Update.empty().set("firstName", "foo"), persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).isEqualTo("first_name = 'foo'");
	}

	@Test // DATACASS-487
	public void shouldReplaceUdtMap() {

		Manufacturer manufacturer = new Manufacturer("foobar");

		Map<Manufacturer, Currency> map = Collections.singletonMap(manufacturer, currency);

		Update update = Update.empty().set("manufacturers", map);

		Update mappedUpdate = updateMapper.getMappedObject(update, persistentEntity);

		assertThat(mappedUpdate.getUpdateOperations()).hasSize(1);
		assertThat(mappedUpdate.toString()).isEqualTo("manufacturers = {{name:'foobar'}:'Euro'}");
	}

	@Test // DATACASS-343
	public void shouldCreateSetAtIndexUpdate() {

		Update update = updateMapper.getMappedObject(Update.empty().set("list").atIndex(10).to(currency), persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).isEqualTo("list[10] = 'Euro'");
	}

	@Test // DATACASS-343
	public void shouldCreateSetAtKeyUpdate() {

		Update update = updateMapper.getMappedObject(Update.empty().set("map").atKey("baz").to(currency), persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).isEqualTo("map['baz'] = 'Euro'");
	}

	@Test // DATACASS-487
	public void shouldCreateSetAtUdtKeyUpdate() {

		Manufacturer manufacturer = new Manufacturer("foobar");

		Update update = updateMapper.getMappedObject(Update.empty().set("manufacturers").atKey(manufacturer).to(currency),
				persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).isEqualTo("manufacturers[{name:'foobar'}] = 'Euro'");
	}

	@Test // DATACASS-343
	public void shouldAddToMap() {

		Update update = updateMapper.getMappedObject(Update.empty().addTo("map").entry("foo", currency), persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).isEqualTo("map = map + {'foo':'Euro'}");
	}

	@Test // DATACASS-487
	public void shouldAddUdtToMap() {

		Manufacturer manufacturer = new Manufacturer("foobar");

		Update update = Update.empty().addTo("manufacturers").entry(manufacturer, currency);

		Update mappedUpdate = updateMapper.getMappedObject(update, persistentEntity);

		assertThat(mappedUpdate.getUpdateOperations()).hasSize(1);
		assertThat(mappedUpdate.toString()).isEqualTo("manufacturers = manufacturers + {{name:'foobar'}:'Euro'}");
	}

	@Test // DATACASS-343
	public void shouldPrependAllToList() {

		Update update = updateMapper.getMappedObject(Update.empty().addTo("list").prependAll("foo", currency),
				persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).isEqualTo("list = ['foo','Euro'] + list");
	}

	@Test // DATACASS-343
	public void shouldAppendAllToList() {

		Update update = updateMapper.getMappedObject(Update.empty().addTo("list").appendAll("foo", currency),
				persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).isEqualTo("list = list + ['foo','Euro']");
	}

	@Test // DATACASS-343
	public void shouldRemoveFromList() {

		Update update = updateMapper.getMappedObject(Update.empty().remove("list", currency), persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).isEqualTo("list = list - ['Euro']");
	}

	@Test // DATACASS-343
	public void shouldClearList() {

		Update update = updateMapper.getMappedObject(Update.empty().clear("list"), persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).isEqualTo("list = []");
	}

	@Test // DATACASS-343
	public void shouldClearSet() {

		Update update = updateMapper.getMappedObject(Update.empty().clear("set"), persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).isEqualTo("set_col = {}");
	}

	@Test // DATACASS-343
	public void shouldCreateIncrementUpdate() {

		Update update = updateMapper.getMappedObject(Update.empty().increment("number"), persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).isEqualTo("number = number + 1");
	}

	@Test // DATACASS-343
	public void shouldCreateDecrementUpdate() {

		Update update = updateMapper.getMappedObject(Update.empty().decrement("number"), persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).isEqualTo("number = number - 1");
	}

	@Test // DATACASS-523
	public void shouldMapTuple() {

		Update update = this.updateMapper.getMappedObject(Update.empty().set("tuple", new MappedTuple("foo")),
				this.persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).isEqualTo("tuple = ('foo')");
	}

	@Test // DATACASS-302, DATACASS-694
	public void shouldMapTime() {

		Update update = this.updateMapper.getMappedObject(Update.empty().set("localTime", LocalTime.of(1, 2, 3)),
				this.persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).startsWith("localtime = '01:02:03.");
	}

	@Test // DATACASS-523
	public void referencingTupleElementsInQueryShouldFail() {
		assertThatIllegalArgumentException().isThrownBy(
				() -> this.updateMapper.getMappedObject(Update.empty().set("tuple.zip", "bar"), this.persistentEntity));
	}

	@Test // DATACASS-167
	public void shouldMapEmbeddedEntity() {

		Update update = this.updateMapper.getMappedObject(Update.empty().set("nested.firstname", "spring"),
				mappingContext.getRequiredPersistentEntity(WithNullableEmbeddedType.class));

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).startsWith("firstname = 'spring'");
	}

	@Test // DATACASS-167
	public void shouldMapPrefixedEmbeddedEntity() {

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
