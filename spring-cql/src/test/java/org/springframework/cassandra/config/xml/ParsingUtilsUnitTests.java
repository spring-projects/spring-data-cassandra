/*
 *  Copyright 2013-2016 the original author or authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.springframework.cassandra.config.xml;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.springframework.cassandra.support.BeanDefinitionTestUtils.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;

/**
 * Test suite of unit tests testing the contract and functionality of the {@link ParsingUtils} class.
 *
 * @author John Blum
 */
// TODO: add more tests!
public class ParsingUtilsUnitTests {

	@Rule public ExpectedException exception = ExpectedException.none();

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-298">DATACASS-298</a>
	 */
	@Test
	public void addOptionalReferencePropertyUsesDefault() {

		BeanDefinitionBuilder builder = ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(),
				"referenceProperty", null, "defaultBeanReference", false, true);

		RuntimeBeanReference propertyValue = getPropertyValue(builder.getBeanDefinition(), "referenceProperty");

		assertThat(propertyValue, is(notNullValue(RuntimeBeanReference.class)));
		assertThat(propertyValue.getBeanName(), is(equalTo("defaultBeanReference")));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-298">DATACASS-298</a>
	 */
	@Test
	public void addOptionalReferencePropertyWithNoValueDoesReturnsWithoutAdding() {

		BeanDefinitionBuilder builder = ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(),
				"referenceProperty", null, null, false, false);

		BeanDefinition beanDefinition = builder.getRawBeanDefinition();

		assertThat(beanDefinition.getPropertyValues().contains("referenceProperty"), is(false));
		assertThat(beanDefinition.getPropertyValues().isEmpty(), is(true));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-298">DATACASS-298</a>
	 */
	@Test
	public void addOptionalValuePropertyUsesDefault() {

		BeanDefinitionBuilder builder = ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(),
				"valueProperty", null, "defaultValue", false, false);

		String propertyValue = getPropertyValue(builder.getBeanDefinition(), "valueProperty");

		assertThat(propertyValue, is(equalTo("defaultValue")));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-298">DATACASS-298</a>
	 */
	@Test
	public void addOptionalValuePropertyWithNoValueDoesReturnsWithoutAdding() {

		BeanDefinitionBuilder builder = ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(),
				"valueProperty", null, null, false, false);

		BeanDefinition beanDefinition = builder.getRawBeanDefinition();

		assertThat(beanDefinition.getPropertyValues().contains("valueProperty"), is(false));
		assertThat(beanDefinition.getPropertyValues().isEmpty(), is(true));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-298">DATACASS-298</a>
	 */
	@Test
	public void addRequiredReferencePropertyIsSuccessful() {

		BeanDefinitionBuilder builder = ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(),
				"referenceProperty", "reference", null, true, true);

		RuntimeBeanReference propertyValue = getPropertyValue(builder.getBeanDefinition(), "referenceProperty");

		assertThat(propertyValue, is(notNullValue(RuntimeBeanReference.class)));
		assertThat(propertyValue.getBeanName(), is(equalTo("reference")));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-298">DATACASS-298</a>
	 */
	@Test
	public void addRequiredReferencePropertyWithNoReferenceFails() {

		exception.expect(IllegalArgumentException.class);
		exception.expectCause(is(nullValue(Throwable.class)));
		exception.expectMessage("value required for property reference [referenceProperty] on class [null]");

		ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(), "referenceProperty", null,
				"defaultReference", true, true);
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-298">DATACASS-298</a>
	 */
	@Test
	public void addRequiredValuePropertyIsSuccessful() {

		BeanDefinitionBuilder builder = ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(),
				"valueProperty", "value", null, true, false);

		String propertyValue = getPropertyValue(builder.getBeanDefinition(), "valueProperty");

		assertThat(propertyValue, is(equalTo("value")));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-298">DATACASS-298</a>
	 */
	@Test
	public void addRequiredValuePropertyWithNoValueFails() {

		exception.expect(IllegalArgumentException.class);
		exception.expectCause(is(nullValue(Throwable.class)));
		exception.expectMessage("value required for property [valueProperty] on class [null]");

		ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(), "valueProperty", null, "defaultValue", true,
				false);
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-298">DATACASS-298</a>
	 */
	@Test
	public void addPropertyThrowsIllegalArgumentExceptionForNullBuilder() {

		exception.expect(IllegalArgumentException.class);
		exception.expectCause(is(nullValue(Throwable.class)));
		exception.expectMessage("BeanDefinitionBuilder must not be null");

		ParsingUtils.addProperty(null, "propertyName", "value", "defaultValue", false, false);
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-298">DATACASS-298</a>
	 */
	@Test
	public void addPropertyThrowsIllegalArgumentExceptionForNullPropertyName() {

		exception.expect(IllegalArgumentException.class);
		exception.expectCause(is(nullValue(Throwable.class)));
		exception.expectMessage("Property name must not be null");

		ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(), null, "value", "defaultValue", false, true);
	}
}
