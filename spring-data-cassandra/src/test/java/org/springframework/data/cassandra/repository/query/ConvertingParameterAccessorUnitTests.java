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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.time.LocalDate;
import java.util.Arrays;
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

	@Mock CassandraParameterAccessor delegateMock;
	@Mock CassandraPersistentProperty propertyMock;
	MappingCassandraConverter converter;
	ConvertingParameterAccessor accessor;

	@Before
	public void setUp() {

		this.converter = new MappingCassandraConverter(new BasicCassandraMappingContext());
		this.converter.afterPropertiesSet();
		this.accessor = new ConvertingParameterAccessor(converter, delegateMock);
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReturnNullBindableValue() {
		assertThat(accessor.getBindableValue(0), is(nullValue()));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	@SuppressWarnings({"rawtypes", "unchecked"})
	public void shouldReturnNativeBindableValue() {

		when(delegateMock.getBindableValue(0)).thenReturn("hello");
		when(delegateMock.getDataType(0)).thenReturn(DataType.varchar());
		when(delegateMock.getParameterType(0)).thenReturn((Class) String.class);

		assertThat(accessor.getBindableValue(0), is(equalTo((Object) "hello")));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	@SuppressWarnings({"rawtypes", "unchecked"})
	public void shouldReturnConvertedBindableValue() {

		LocalDate localDate = LocalDate.of(2010, 7, 4);

		when(delegateMock.getBindableValue(0)).thenReturn(localDate);
		when(delegateMock.getParameterType(0)).thenReturn((Class) LocalDate.class);

		assertThat(accessor.getBindableValue(0),
				is(equalTo((Object) com.datastax.driver.core.LocalDate.fromYearMonthDay(2010, 7, 4))));
	}

	/**
	 * @see DATACASS-296
	 * @see DATACASS-7
	 */
	@Test
	public void shouldReturnDataTypeProvidedByDelegate() {

		when(delegateMock.getDataType(0)).thenReturn(DataType.varchar());

		assertThat(accessor.getDataType(0), is(equalTo(DataType.varchar())));
	}

	/**
	 * @see DATACASS-296
	 * @see DATACASS-7
	 */
	@Test
	@SuppressWarnings({"rawtypes", "unchecked"})
	public void shouldConvertCollections() {

		LocalDate localDate = LocalDate.of(2010, 7, 4);

		when(delegateMock.iterator()).thenReturn((Iterator) Arrays.asList(Collections.singletonList(localDate)).iterator());
		when(delegateMock.getDataType(0)).thenReturn(DataType.list(DataType.date()));
		when(delegateMock.getParameterType(0)).thenReturn((Class) List.class);
		when(propertyMock.getType()).thenReturn((Class) List.class);
		when(propertyMock.getActualType()).thenReturn((Class) LocalDate.class);
		when(propertyMock.isCollectionLike()).thenReturn(true);

		PotentiallyConvertingIterator iterator = (PotentiallyConvertingIterator) accessor.iterator();
		Object converted = iterator.nextConverted(propertyMock);

		assertThat(converted, is(instanceOf(List.class)));

		List<?> list = (List<?>) converted;
		assertThat(list.get(0), is(instanceOf(com.datastax.driver.core.LocalDate.class)));
	}

	/**
	 * @see DATACASS-7
	 */
	@Test
	@SuppressWarnings({"rawtypes", "unchecked"})
	public void shouldProvideTypeBasedOnValue() {

		when(delegateMock.getDataType(0)).thenReturn(null);
		when(delegateMock.getParameterType(0)).thenReturn((Class) LocalDate.class);

		assertThat(accessor.getDataType(0), is(equalTo(DataType.date())));
	}

	/**
	 * @see DATACASS-7
	 */
	@Test
	@SuppressWarnings("rawtypes")
	public void shouldProvideTypeBasedOnPropertyType() {

		when(propertyMock.getDataType()).thenReturn(DataType.varchar());
		when(propertyMock.findAnnotation(CassandraType.class)).thenReturn(mock(CassandraType.class));
		when(delegateMock.getParameterType(0)).thenReturn((Class) String.class);
		when(delegateMock.getDataType(0)).thenReturn(null);

		assertThat(accessor.getDataType(0, propertyMock), is(equalTo(DataType.varchar())));
	}
}
