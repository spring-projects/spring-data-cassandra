/*
 * Copyright 2018 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.springframework.data.cassandra.test.util.RowMockUtil.column;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Data;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;
import org.springframework.data.cassandra.core.mapping.UserTypeResolver;
import org.springframework.data.cassandra.support.UserTypeBuilder;
import org.springframework.data.cassandra.test.util.RowMockUtil;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Unit tests for UDT through {@link MappingCassandraConverter}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.Silent.class) // there are some unused stubbings in RowMockUtil but they're used in other
public class MappingCassandraConverterUDTUnitTests {

	@Rule public final ExpectedException expectedException = ExpectedException.none();
	@Mock UserTypeResolver userTypeResolver;

	UserType manufacturer = UserTypeBuilder.forName("manufacturer").withField("name", DataType.varchar()).build();
	UserType currency = UserTypeBuilder.forName("mycurrency").withField("currency", DataType.varchar()).build();

	Row rowMock;

	CassandraMappingContext mappingContext;
	MappingCassandraConverter mappingCassandraConverter;

	@Before
	public void setUp() {

		mappingContext = new CassandraMappingContext();
		mappingContext.setUserTypeResolver(userTypeResolver);

		mappingCassandraConverter = new MappingCassandraConverter(mappingContext);
		mappingCassandraConverter.afterPropertiesSet();

		when(userTypeResolver.resolveType(CqlIdentifier.of("manufacturer"))).thenReturn(manufacturer);
		when(userTypeResolver.resolveType(CqlIdentifier.of("currency"))).thenReturn(currency);
	}

	@Test // DATACASS-487
	public void shouldReadMappedUdtInMap() {

		UDTValue key = manufacturer.newValue().setString("name", "a good one");
		UDTValue value1 = currency.newValue().setString("currency", "EUR");
		UDTValue value2 = currency.newValue().setString("currency", "USD");

		Map<UDTValue, List<UDTValue>> map = new HashMap<>();

		map.put(key, Arrays.asList(value1, value2));

		rowMock = RowMockUtil.newRowMock(column("acceptedCurrencies", map, DataType.map(manufacturer, DataType.list(currency))));

		Supplier supplier = mappingCassandraConverter.read(Supplier.class, rowMock);

		assertThat(supplier.getAcceptedCurrencies()).isNotEmpty();

		List<Currency> currencies = supplier.getAcceptedCurrencies().get(new Manufacturer("a good one"));

		assertThat(currencies).contains(new Currency("EUR"), new Currency("USD"));
	}

	@Test // DATACASS-487
	public void shouldWriteMappedUdtInMap() {

		Map<Manufacturer, List<Currency>> currencies = Collections.singletonMap(new Manufacturer("a good one"),
				Arrays.asList(new Currency("EUR"), new Currency("USD")));

		Supplier supplier = new Supplier(currencies);

		Insert insert = QueryBuilder.insertInto("table");

		mappingCassandraConverter.write(supplier, insert);

		assertThat(insert.toString()).contains("VALUES ({{name:'a good one'}:[{currency:'EUR'},{currency:'USD'}]}");
	}

	@UserDefinedType
	@Data
	@AllArgsConstructor
	private static class Manufacturer {
		String name;
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
}
