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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;

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

	MappingCassandraConverter converter;

	@Before
	public void setUp() {

		this.converter = new MappingCassandraConverter(new BasicCassandraMappingContext());
		this.converter.afterPropertiesSet();
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReturnNullBindableValue() {

		ConvertingParameterAccessor accessor = new ConvertingParameterAccessor(converter, delegateMock);

		assertThat(accessor.getBindableValue(0), is(nullValue()));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReturnNativeBindableValue() {

		ConvertingParameterAccessor accessor = new ConvertingParameterAccessor(converter, delegateMock);

		when(delegateMock.getBindableValue(0)).thenReturn("hello");
		when(delegateMock.getDataType(0)).thenReturn(DataType.varchar());

		assertThat(accessor.getBindableValue(0), is(equalTo((Object) "hello")));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	@SuppressWarnings("rawtypes")
	public void shouldReturnConvertedBindableValue() {

		ConvertingParameterAccessor accessor = new ConvertingParameterAccessor(converter, delegateMock);

		LocalDate localDate = LocalDate.of(2010, 7, 4);

		when(delegateMock.getBindableValue(0)).thenReturn(localDate);
		when(delegateMock.getParameterType(0)).thenReturn((Class) LocalDate.class);

		assertThat(accessor.getBindableValue(0),
				is(equalTo((Object) com.datastax.driver.core.LocalDate.fromYearMonthDay(2010, 7, 4))));
	}
}
