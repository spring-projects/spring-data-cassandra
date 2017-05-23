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

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import org.junit.Test;

/**
 * Unit tests for class {@link PropertyMapping org.springframework.data.cassandra.mapping.PropertyMapping}.
 *
 * @author Michael Hausegger, hausegger.michael@googlemail.com
 */
public class PropertyMappingTest {


	@Test  //DATACASS-405
	public void testSetPropertyName() throws Exception {

		PropertyMapping propertyMapping = new PropertyMapping("a", "b", "c");
		propertyMapping.setPropertyName("d");

		assertThat(propertyMapping.getForceQuote()).isEqualTo("c");
		assertThat(propertyMapping.getPropertyName()).isEqualTo("d");

		assertThat(propertyMapping.getColumnName()).isEqualTo("b");
	}


	@Test  //DATACASS-405
	public void testSetColumnName() throws Exception {

		PropertyMapping propertyMapping = new PropertyMapping("a", "b", "c");
		propertyMapping.setColumnName("d");

		assertThat(propertyMapping.getPropertyName()).isEqualTo("a");
		assertThat(propertyMapping.getForceQuote()).isEqualTo("c");

		assertThat(propertyMapping.getColumnName()).isEqualTo("d");
	}


	@Test(expected = IllegalArgumentException.class)  //DATACASS-405
	public void testFailsToCreatePropertyMappingTakingWithThreeArgumentsThrowsIllegalArgumentException() throws Exception {

		new PropertyMapping(null, "false", "");
	}


	@Test(expected = IllegalArgumentException.class)    //DATACASS-405
	public void testFailsToCreatePropertyMappingTakingTwoArgumentsThrowsIllegalArgumentException() throws Exception {

		new PropertyMapping(null, null);
	}


	@Test(expected = IllegalArgumentException.class)  //DATACASS-405
	public void testFailsToCreatePropertyMappingTakingStringThrowsIllegalArgumentException() throws Exception {

		new PropertyMapping(null);
	}
}