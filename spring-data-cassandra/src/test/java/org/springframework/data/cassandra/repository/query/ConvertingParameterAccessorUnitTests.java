/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.cassandra.repository.query;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.LocalDate;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.mapping.CassandraType;
import org.springframework.data.cassandra.repository.query.ConvertingParameterAccessor.PotentiallyConvertingIterator;

import com.datastax.driver.core.DataType;

/**
 * Unit tests for {@link ConvertingParameterAccessor}.
 *
 * @author Mark Paluch
 */
@SuppressWarnings("Since15")
@RunWith(MockitoJUnitRunner.class)
public class ConvertingParameterAccessorUnitTests {

	@Mock CassandraParameterAccessor mockParameterAccessor;

	@Mock CassandraPersistentProperty mockProperty;

	ConvertingParameterAccessor convertingParameterAccessor;

	MappingCassandraConverter converter;

	@Before
	public void setUp() {
		this.converter = new MappingCassandraConverter(new BasicCassandraMappingContext());
		this.converter.afterPropertiesSet();
		this.convertingParameterAccessor = new ConvertingParameterAccessor(converter, mockParameterAccessor);
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-296">DATACASS-296</a>
	 */
	@Test
	public void shouldReturnNullBindableValue() {

		ConvertingParameterAccessor accessor = new ConvertingParameterAccessor(converter, mockParameterAccessor);

		assertThat(accessor.getBindableValue(0)).isNull();
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-296">DATACASS-296</a>
	 */
	@Test
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void shouldReturnNativeBindableValue() {
		when(mockParameterAccessor.getBindableValue(0)).thenReturn("hello");
		when(mockParameterAccessor.getDataType(0)).thenReturn(DataType.varchar());
		when(mockParameterAccessor.getParameterType(0)).thenReturn((Class) String.class);

		ConvertingParameterAccessor accessor = new ConvertingParameterAccessor(converter, mockParameterAccessor);

		when(mockParameterAccessor.getBindableValue(0)).thenReturn("hello");
		when(mockParameterAccessor.getDataType(0)).thenReturn(DataType.varchar());

		assertThat(accessor.getBindableValue(0)).isEqualTo((Object) "hello");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-296">DATACASS-296</a>
	 */
	@Test
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void shouldReturnConvertedBindableValue() {
		LocalDate localDate = LocalDate.of(2010, 7, 4);

		when(mockParameterAccessor.getBindableValue(0)).thenReturn(localDate);
		when(mockParameterAccessor.getParameterType(0)).thenReturn((Class) LocalDate.class);

		assertThat(convertingParameterAccessor.getBindableValue(0))
				.isEqualTo(com.datastax.driver.core.LocalDate.fromYearMonthDay(2010, 7, 4));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-296">DATACASS-296</a>
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void shouldReturnDataTypeProvidedByDelegate() {
		when(mockParameterAccessor.getDataType(0)).thenReturn(DataType.varchar());

		assertThat(convertingParameterAccessor.getDataType(0)).isEqualTo(DataType.varchar());
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-296">DATACASS-296</a>
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void shouldConvertCollections() {
		LocalDate localDate = LocalDate.of(2010, 7, 4);

		when(mockParameterAccessor.iterator())
				.thenReturn((Iterator) Collections.singletonList(Collections.singletonList(localDate)).iterator());
		when(mockParameterAccessor.getDataType(0)).thenReturn(DataType.list(DataType.date()));
		when(mockParameterAccessor.getParameterType(0)).thenReturn((Class) List.class);
		when(mockProperty.getType()).thenReturn((Class) List.class);
		when(mockProperty.getActualType()).thenReturn((Class) LocalDate.class);
		when(mockProperty.isCollectionLike()).thenReturn(true);

		PotentiallyConvertingIterator iterator = (PotentiallyConvertingIterator) convertingParameterAccessor.iterator();
		Object converted = iterator.nextConverted(mockProperty);

		assertThat(converted).isInstanceOf(List.class);

		List<?> list = (List<?>) converted;

		assertThat(list.get(0)).isInstanceOf(com.datastax.driver.core.LocalDate.class);
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void shouldProvideTypeBasedOnValue() {
		when(mockParameterAccessor.getDataType(0)).thenReturn(null);
		when(mockParameterAccessor.getParameterType(0)).thenReturn((Class) LocalDate.class);

		assertThat(convertingParameterAccessor.getDataType(0)).isEqualTo(DataType.date());
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void shouldProvideTypeBasedOnPropertyType() {
		when(mockProperty.getDataType()).thenReturn(DataType.varchar());
		when(mockProperty.findAnnotation(CassandraType.class)).thenReturn(mock(CassandraType.class));
		when(mockParameterAccessor.getParameterType(0)).thenReturn((Class) String.class);
		when(mockParameterAccessor.getDataType(0)).thenReturn(null);

		assertThat(convertingParameterAccessor.getDataType(0, mockProperty)).isEqualTo(DataType.varchar());
	}
}
