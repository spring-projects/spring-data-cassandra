/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.cassandra.core.convert;

import static org.assertj.core.api.Assertions.*;

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
import org.springframework.data.cassandra.core.mapping.UserTypeResolver;
import org.springframework.data.cassandra.core.query.Update;

/**
 * Unit tests for {@link UpdateMapper}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class UpdateMapperUnitTests {

	CassandraMappingContext mappingContext = new CassandraMappingContext();
	CassandraPersistentEntity<?> persistentEntity;
	MappingCassandraConverter cassandraConverter;
	UpdateMapper updateMapper;

	@Mock UserTypeResolver userTypeResolver;

	Currency currency = Currency.getInstance("EUR");

	@Before
	public void before() throws Exception {

		CassandraCustomConversions customConversions = new CassandraCustomConversions(
				Collections.singletonList(CurrencyConverter.INSTANCE));

		mappingContext.setCustomConversions(customConversions);
		mappingContext.setUserTypeResolver(userTypeResolver);

		cassandraConverter = new MappingCassandraConverter(mappingContext);
		cassandraConverter.setCustomConversions(customConversions);
		cassandraConverter.afterPropertiesSet();

		updateMapper = new UpdateMapper(cassandraConverter);

		persistentEntity = mappingContext.getRequiredPersistentEntity(Person.class);
	}

	@Test // DATACASS-343
	public void shouldCreateSimpleUpdate() {

		Update update = updateMapper.getMappedObject(Update.empty().set("firstName", "foo"), persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).isEqualTo("first_name = 'foo'");
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

	@Test // DATACASS-343
	public void shouldAddToMap() {

		Update update = updateMapper.getMappedObject(Update.empty().addTo("map").entry("foo", currency), persistentEntity);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).isEqualTo("map = map + {'foo':'Euro'}");
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

	static class Person {

		@Id String id;

		List<Currency> list;
		@Column("set_col") Set<Currency> set;
		Map<String, Currency> map;
		Currency currency;

		Integer number;

		@Column("first_name") String firstName;
	}
}
