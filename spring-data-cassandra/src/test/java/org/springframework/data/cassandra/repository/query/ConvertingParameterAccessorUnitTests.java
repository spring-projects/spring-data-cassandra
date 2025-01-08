/*
 * Copyright 2016-2025 the original author or authors.
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
package org.springframework.data.cassandra.repository.query;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.LocalDate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;

import com.datastax.oss.driver.api.core.type.DataTypes;

/**
 * Unit tests for {@link ConvertingParameterAccessor}.
 *
 * @author Mark Paluch
 */
@SuppressWarnings("Since15")
@ExtendWith(MockitoExtension.class)
class ConvertingParameterAccessorUnitTests {

	@Mock CassandraParameterAccessor mockParameterAccessor;
	@Mock CassandraPersistentProperty mockProperty;

	private ConvertingParameterAccessor convertingParameterAccessor;
	private MappingCassandraConverter converter;

	@BeforeEach
	void setUp() {

		this.converter = new MappingCassandraConverter();
		this.converter.afterPropertiesSet();
		this.convertingParameterAccessor = new ConvertingParameterAccessor(converter, mockParameterAccessor);
	}

	@Test // DATACASS-296
	void shouldReturnNullBindableValue() {

		ConvertingParameterAccessor accessor = new ConvertingParameterAccessor(converter, mockParameterAccessor);

		assertThat(accessor.getBindableValue(0)).isNull();
	}

	@Test // DATACASS-296
	@SuppressWarnings({ "rawtypes", "unchecked" })
	void shouldReturnNativeBindableValue() {

		when(mockParameterAccessor.getBindableValue(0)).thenReturn("hello");

		ConvertingParameterAccessor accessor = new ConvertingParameterAccessor(converter, mockParameterAccessor);

		assertThat(accessor.getBindableValue(0)).isEqualTo((Object) "hello");
	}

	@Test // DATACASS-296
	void shouldReturnConvertedBindableValue() {

		LocalDate localDate = LocalDate.of(2010, 7, 4);

		when(mockParameterAccessor.getBindableValue(0)).thenReturn(localDate);

		assertThat(convertingParameterAccessor.getBindableValue(0)).isEqualTo(LocalDate.of(2010, 7, 4));
	}

	@Test // DATACASS-296, DATACASS-7
	void shouldReturnDataTypeProvidedByDelegate() {

		when(mockParameterAccessor.getDataType(0)).thenReturn(DataTypes.TEXT);

		assertThat(convertingParameterAccessor.getDataType(0)).isEqualTo(DataTypes.TEXT);
	}

}
