/*
 * Copyright 2016-2018 the original author or authors.
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
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.repository.query.ConvertingParameterAccessor.PotentiallyConvertingIterator;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;

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

		this.converter = new MappingCassandraConverter();
		this.converter.afterPropertiesSet();
		this.convertingParameterAccessor = new ConvertingParameterAccessor(converter, mockParameterAccessor);
	}

	@Test // DATACASS-296
	public void shouldReturnNullBindableValue() {

		ConvertingParameterAccessor accessor = new ConvertingParameterAccessor(converter, mockParameterAccessor);

		assertThat(accessor.getBindableValue(0)).isNull();
	}

	@Test // DATACASS-296
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void shouldReturnNativeBindableValue() {

		when(mockParameterAccessor.getBindableValue(0)).thenReturn("hello");

		ConvertingParameterAccessor accessor = new ConvertingParameterAccessor(converter, mockParameterAccessor);

		assertThat(accessor.getBindableValue(0)).isEqualTo((Object) "hello");
	}

	@Test // DATACASS-296
	public void shouldReturnConvertedBindableValue() {

		LocalDate localDate = LocalDate.of(2010, 7, 4);

		when(mockParameterAccessor.getBindableValue(0)).thenReturn(localDate);

		assertThat(convertingParameterAccessor.getBindableValue(0))
				.isEqualTo(com.datastax.driver.core.LocalDate.fromYearMonthDay(2010, 7, 4));
	}

	@Test // DATACASS-296, DATACASS-7
	public void shouldReturnDataTypeProvidedByDelegate() {

		when(mockParameterAccessor.getDataType(0)).thenReturn(DataType.varchar());

		assertThat(convertingParameterAccessor.getDataType(0)).isEqualTo(DataType.varchar());
	}

	@Test // DATACASS-296, DATACASS-7
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void shouldConvertCollections() {

		LocalDate localDate = LocalDate.of(2010, 7, 4);

		when(mockParameterAccessor.iterator())
				.thenReturn((Iterator) Collections.singletonList(Collections.singletonList(localDate)).iterator());
		when(mockProperty.getTypeInformation()).thenReturn((TypeInformation) ClassTypeInformation.LIST);

		PotentiallyConvertingIterator iterator = (PotentiallyConvertingIterator) convertingParameterAccessor.iterator();
		Object converted = iterator.nextConverted(mockProperty);

		assertThat(converted).isInstanceOf(List.class);

		List<?> list = (List<?>) converted;

		assertThat(list.get(0)).isInstanceOf(com.datastax.driver.core.LocalDate.class);
	}

	@Test // DATACASS-7, DATACASS-506
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void shouldProvideTypeBasedOnPropertyType() {

		when(mockProperty.getDataType()).thenReturn(DataType.varchar());
		when(mockProperty.isAnnotationPresent(CassandraType.class)).thenReturn(true);
		when(mockProperty.getRequiredAnnotation(CassandraType.class)).thenReturn(mock(CassandraType.class));
		when(mockParameterAccessor.getParameterType(0)).thenReturn((Class) String.class);

		assertThat(convertingParameterAccessor.getDataType(0, mockProperty)).isEqualTo(DataType.varchar());
	}
}
