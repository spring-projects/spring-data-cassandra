/*
 * Copyright 2013-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.config;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;

/**
 * Utilities to lookup {@link BeanDefinition bean definitions} for a {@link ListableBeanFactory} and to conditionally
 * register {@link BeanDefinition bean definitions}.
 *
 * @author Matthew Adams
 * @author Mark Paluch
 */
class BeanDefinitionUtils {

	/**
	 * Returns all {@link BeanDefinitionHolder}s with the given type.
	 *
	 * @param registry The {@link BeanDefinitionRegistry}, often the very same instance as the {@code factor} parameter.
	 * @param factory The {@link ListableBeanFactory}, often the very same instance as the {@code registry} parameter.
	 * @param type The required {@link BeanDefinition}'s type.
	 * @param includeNonSingletons Whether to include beans with scope other than {@code singleton}
	 * @param allowEagerInit Whether to allow eager initialization of beans.
	 * @return The {@link BeanDefinitionHolder}s -- never returns null.
	 * @see BeanFactoryUtils#beanNamesForTypeIncludingAncestors(ListableBeanFactory, Class, boolean, boolean)
	 */
	static BeanDefinitionHolder[] getBeanDefinitionsOfType(BeanDefinitionRegistry registry, ListableBeanFactory factory,
			Class<?> type, boolean includeNonSingletons, boolean allowEagerInit) {

		String[] names = BeanFactoryUtils.beanNamesForTypeIncludingAncestors(factory, type, includeNonSingletons,
				allowEagerInit);

		if (names.length == 0) {
			return new BeanDefinitionHolder[] {};
		}

		BeanDefinitionHolder[] array = new BeanDefinitionHolder[names.length];

		for (int i = 0; i < names.length; i++) {

			String name = names[i];
			BeanDefinition beanDefinition = null;

			while (beanDefinition == null) {
				try {
					beanDefinition = registry.getBeanDefinition(name);
				} catch (NoSuchBeanDefinitionException x) {
					if (FactoryBean.class.isAssignableFrom(type)) { // try unmanged BeanFactory-prefixed name
						name = name.substring(BeanFactory.FACTORY_BEAN_PREFIX.length());
					} else {
						throw x;
					}
				}
			}

			array[i] = new BeanDefinitionHolder(beanDefinition, name);
		}

		return array;
	}
}
