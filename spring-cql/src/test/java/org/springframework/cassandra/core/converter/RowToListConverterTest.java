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

package org.springframework.cassandra.core.converter;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import org.junit.Test;

/**
 * Unit tests for class {@link RowToListConverter org.springframework.cassandra.core.converter.RowToListConverter}.
 *
 * @author Michael Hausegger, hausegger.michael@googlemail.com
 */
public class RowToListConverterTest {


	@Test
	public void testCreatesRowToListConverterAndCallsConvert() throws Exception {

		RowToListConverter rowToMapConverter = new RowToListConverter();

		assertThat(rowToMapConverter.convert(null)).isNull();
	}
}