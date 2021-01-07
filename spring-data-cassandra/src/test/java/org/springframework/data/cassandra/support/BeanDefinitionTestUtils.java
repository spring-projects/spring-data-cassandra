/*
 * Copyright 2016-2021 the original author or authors.
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
package org.springframework.data.cassandra.support;

import org.springframework.beans.PropertyValue;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.util.Assert;

/**
 * {@code BeanDefinitionTestUtils} is a collection of {@link org.springframework.beans.factory.config.BeanDefinition}
 * -based utility methods for use in unit and integration testing scenarios.
 *
 * @author Mark Paluch
 */
public abstract class BeanDefinitionTestUtils {

	/**
	 * Prevent instances.
	 */
	private BeanDefinitionTestUtils() {}

	/**
	 * Retrieve the {@code propertyValue} from a {@link BeanDefinition} by its {@code propertyName}.
	 *
	 * @param beanDefinition must not be {@literal null}.
	 * @param propertyName must not be {@literal null} or empty.
	 * @return the property value, may be {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	public static <T> T getPropertyValue(BeanDefinition beanDefinition, String propertyName) {

		Assert.notNull(beanDefinition, "BeanDefinition must not be null");
		Assert.notNull(propertyName, "Property name must not be empty");

		PropertyValue propertyValue = beanDefinition.getPropertyValues().getPropertyValue(propertyName);

		return (T) (propertyValue != null ? propertyValue.getValue() : null);
	}

	/**
	 * Retrieve the {@code propertyValue} as {@literal String} from a {@link BeanDefinition} by its {@code propertyName}.
	 *
	 * @param beanDefinition must not be {@literal null}.
	 * @param propertyName must not be {@literal null} or empty.
	 * @return the property value, may be {@literal null}.
	 */
	public static String getPropertyValueAsString(BeanDefinition beanDefinition, String propertyName) {

		Assert.notNull(beanDefinition, "BeanDefinition must not be null");
		Assert.notNull(propertyName, "Property name must not be empty");

		Object value = getPropertyValue(beanDefinition, propertyName);

		return (value instanceof RuntimeBeanReference ? ((RuntimeBeanReference) value).getBeanName()
				: (value != null ? String.valueOf(value) : null));
	}
}
