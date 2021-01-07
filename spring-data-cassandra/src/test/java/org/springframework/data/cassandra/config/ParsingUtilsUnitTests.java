/*
 * Copyright 2013-2021 the original author or authors.
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

package org.springframework.data.cassandra.config;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.cassandra.support.BeanDefinitionTestUtils.*;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;

/**
 * Test suite of unit tests testing the contract and functionality of the {@link ParsingUtils} class.
 *
 * @author John Blum
 */
class ParsingUtilsUnitTests {

	@Test // DATACASS-298
	void addOptionalReferencePropertyUsesDefault() {

		BeanDefinitionBuilder builder = ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(),
				"referenceProperty", null, "defaultBeanReference", false, true);

		RuntimeBeanReference propertyValue = getPropertyValue(builder.getBeanDefinition(), "referenceProperty");

		assertThat(propertyValue).isNotNull();
		assertThat(propertyValue.getBeanName()).isEqualTo("defaultBeanReference");
	}

	@Test // DATACASS-298
	void addOptionalReferencePropertyWithNoValueDoesReturnsWithoutAdding() {

		BeanDefinitionBuilder builder = ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(),
				"referenceProperty", null, null, false, false);

		BeanDefinition beanDefinition = builder.getRawBeanDefinition();

		assertThat(beanDefinition.getPropertyValues().contains("referenceProperty")).isFalse();
		assertThat(beanDefinition.getPropertyValues().isEmpty()).isTrue();
	}

	@Test // DATACASS-298
	void addOptionalValuePropertyUsesDefault() {

		BeanDefinitionBuilder builder = ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(),
				"valueProperty", null, "defaultValue", false, false);

		String propertyValue = getPropertyValue(builder.getBeanDefinition(), "valueProperty");

		assertThat(propertyValue).isEqualTo("defaultValue");
	}

	@Test // DATACASS-298
	void addOptionalValuePropertyWithNoValueDoesReturnsWithoutAdding() {

		BeanDefinitionBuilder builder = ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(),
				"valueProperty", null, null, false, false);

		BeanDefinition beanDefinition = builder.getRawBeanDefinition();

		assertThat(beanDefinition.getPropertyValues().contains("valueProperty")).isFalse();
		assertThat(beanDefinition.getPropertyValues().isEmpty()).isTrue();
	}

	@Test // DATACASS-298
	void addRequiredReferencePropertyIsSuccessful() {

		BeanDefinitionBuilder builder = ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(),
				"referenceProperty", "reference", null, true, true);

		RuntimeBeanReference propertyValue = getPropertyValue(builder.getBeanDefinition(), "referenceProperty");

		assertThat(propertyValue).isNotNull();
		assertThat(propertyValue.getBeanName()).isEqualTo("reference");
	}

	@Test // DATACASS-298
	void addRequiredReferencePropertyWithNoReferenceFails() {

		assertThatIllegalArgumentException()
				.isThrownBy(() -> ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(), "referenceProperty",
						null, "defaultReference", true, true))
				.withMessageContaining("value required for property reference [referenceProperty] on class [null]");
	}

	@Test // DATACASS-298
	void addRequiredValuePropertyIsSuccessful() {

		BeanDefinitionBuilder builder = ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(),
				"valueProperty", "value", null, true, false);

		String propertyValue = getPropertyValue(builder.getBeanDefinition(), "valueProperty");

		assertThat(propertyValue).isEqualTo("value");
	}

	@Test // DATACASS-298
	void addRequiredValuePropertyWithNoValueFails() {

		assertThatIllegalArgumentException()
				.isThrownBy(() -> ParsingUtils.addProperty(BeanDefinitionBuilder.genericBeanDefinition(), "valueProperty", null,
						"defaultValue", true, false))
				.withMessageContaining("value required for property [valueProperty] on class [null]");
	}

	@Test // DATACASS-298
	void addPropertyThrowsIllegalArgumentExceptionForNullBuilder() {

		assertThatIllegalArgumentException()
				.isThrownBy(() -> ParsingUtils.addProperty(null, "propertyName", "value", "defaultValue", false, false))
				.withMessageContaining("BeanDefinitionBuilder must not be null");
	}

	@Test // DATACASS-298
	void addPropertyThrowsIllegalArgumentExceptionForNullPropertyName() {

		assertThatIllegalArgumentException().isThrownBy(() -> ParsingUtils
				.addProperty(BeanDefinitionBuilder.genericBeanDefinition(), null, "value", "defaultValue", false, true))
				.withMessageContaining("Property name must not be null");
	}
}
