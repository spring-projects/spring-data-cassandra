/*
 * Copyright 2013-2018 the original author or authors.
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

package org.springframework.data.cassandra.config;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.cassandra.support.BeanDefinitionTestUtils.*;

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

	@Test // DATACASS-298
	public void addOptionalReferencePropertyUsesDefault() {

		BeanDefinitionBuilder builder = ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(),
				"referenceProperty", null, "defaultBeanReference", false, true);

		RuntimeBeanReference propertyValue = getPropertyValue(builder.getBeanDefinition(), "referenceProperty");

		assertThat(propertyValue).isNotNull();
		assertThat(propertyValue.getBeanName()).isEqualTo("defaultBeanReference");
	}

	@Test // DATACASS-298
	public void addOptionalReferencePropertyWithNoValueDoesReturnsWithoutAdding() {

		BeanDefinitionBuilder builder = ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(),
				"referenceProperty", null, null, false, false);

		BeanDefinition beanDefinition = builder.getRawBeanDefinition();

		assertThat(beanDefinition.getPropertyValues().contains("referenceProperty")).isFalse();
		assertThat(beanDefinition.getPropertyValues().isEmpty()).isTrue();
	}

	@Test // DATACASS-298
	public void addOptionalValuePropertyUsesDefault() {

		BeanDefinitionBuilder builder = ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(),
				"valueProperty", null, "defaultValue", false, false);

		String propertyValue = getPropertyValue(builder.getBeanDefinition(), "valueProperty");

		assertThat(propertyValue).isEqualTo("defaultValue");
	}

	@Test // DATACASS-298
	public void addOptionalValuePropertyWithNoValueDoesReturnsWithoutAdding() {

		BeanDefinitionBuilder builder = ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(),
				"valueProperty", null, null, false, false);

		BeanDefinition beanDefinition = builder.getRawBeanDefinition();

		assertThat(beanDefinition.getPropertyValues().contains("valueProperty")).isFalse();
		assertThat(beanDefinition.getPropertyValues().isEmpty()).isTrue();
	}

	@Test // DATACASS-298
	public void addRequiredReferencePropertyIsSuccessful() {

		BeanDefinitionBuilder builder = ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(),
				"referenceProperty", "reference", null, true, true);

		RuntimeBeanReference propertyValue = getPropertyValue(builder.getBeanDefinition(), "referenceProperty");

		assertThat(propertyValue).isNotNull();
		assertThat(propertyValue.getBeanName()).isEqualTo("reference");
	}

	@Test // DATACASS-298
	public void addRequiredReferencePropertyWithNoReferenceFails() {

		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("value required for property reference [referenceProperty] on class [null]");

		ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(), "referenceProperty", null,
				"defaultReference", true, true);
	}

	@Test // DATACASS-298
	public void addRequiredValuePropertyIsSuccessful() {

		BeanDefinitionBuilder builder = ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(),
				"valueProperty", "value", null, true, false);

		String propertyValue = getPropertyValue(builder.getBeanDefinition(), "valueProperty");

		assertThat(propertyValue).isEqualTo("value");
	}

	@Test // DATACASS-298
	public void addRequiredValuePropertyWithNoValueFails() {

		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("value required for property [valueProperty] on class [null]");

		ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(), "valueProperty", null, "defaultValue", true,
				false);
	}

	@Test // DATACASS-298
	public void addPropertyThrowsIllegalArgumentExceptionForNullBuilder() {

		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("BeanDefinitionBuilder must not be null");

		ParsingUtils.addProperty(null, "propertyName", "value", "defaultValue", false, false);
	}

	@Test // DATACASS-298
	public void addPropertyThrowsIllegalArgumentExceptionForNullPropertyName() {

		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Property name must not be null");

		ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(), null, "value", "defaultValue", false, true);
	}
}
