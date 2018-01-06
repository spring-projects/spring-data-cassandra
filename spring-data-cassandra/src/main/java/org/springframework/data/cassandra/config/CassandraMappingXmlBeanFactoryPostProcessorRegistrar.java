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

import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.config.BeanComponentDefinitionBuilder;
import org.w3c.dom.Element;

/**
 * Ensures that a {@link CassandraMappingBeanFactoryPostProcessor} is registered.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
@Deprecated
class CassandraMappingXmlBeanFactoryPostProcessorRegistrar {

	/**
	 * Ensures that a {@link CassandraMappingBeanFactoryPostProcessor} is registered. This method is a no-op if one is
	 * already registered.
	 */
	static void ensureRegistration(Element element, ParserContext parserContext) {

		BeanDefinitionRegistry registry = parserContext.getRegistry();
		if (!(registry instanceof GenericApplicationContext)) {
			return;
		}
		ConfigurableListableBeanFactory factory = ((GenericApplicationContext) registry).getBeanFactory();

		String[] names = BeanFactoryUtils.beanNamesForTypeIncludingAncestors(factory,
				CassandraMappingBeanFactoryPostProcessor.class, true, false);
		if (names.length > 0) {
			return;
		}

		BeanComponentDefinitionBuilder componentBuilder = new BeanComponentDefinitionBuilder(element, parserContext);
		BeanDefinitionBuilder definitionBuilder = BeanDefinitionBuilder
				.genericBeanDefinition(CassandraMappingBeanFactoryPostProcessor.class);

		parserContext.registerBeanComponent(componentBuilder.getComponent(definitionBuilder));
	}
}
