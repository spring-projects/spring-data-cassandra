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

package org.springframework.data.cassandra.mapping;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.ArrayList;

import org.junit.Test;
import org.springframework.data.util.TypeInformation;

/**
 * Unit tests for class {@link CassandraSimpleTypeHolder org.springframework.data.cassandra.mapping.CassandraSimpleTypeHolder}.
 *
 * @author Michael Hausegger, hausegger.michael@googlemail.com
 */
public class CassandraSimpleTypeHolderTest {

	@Test(expected = NullPointerException.class)  //DATACASS-405
	public void testGetDataTypeNamesFromThrowsNullPointerException() throws Exception {

		CassandraSimpleTypeHolder.getDataTypeNamesFrom(null);
	}

	@Test  //DATACASS-405
	public void testGetDataTypeNamesFromProvidingEmptyList() throws Exception {

		assertThat(CassandraSimpleTypeHolder.getDataTypeNamesFrom(new ArrayList<TypeInformation<?>>()).length).isEqualTo(0);
	}
}