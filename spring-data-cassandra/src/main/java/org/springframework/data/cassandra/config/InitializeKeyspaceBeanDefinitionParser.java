/*
 * Copyright 2019-2021 the original author or authors.
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

import static org.springframework.data.cassandra.config.ParsingUtils.*;

import java.util.List;

import org.springframework.beans.BeanMetadataElement;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.TypedStringValue;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.cassandra.core.cql.session.init.CompositeKeyspacePopulator;
import org.springframework.data.cassandra.core.cql.session.init.ResourceKeyspacePopulator;
import org.springframework.data.cassandra.core.cql.session.init.SessionFactoryInitializer;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;

import org.w3c.dom.Element;

/**
 * {@link org.springframework.beans.factory.xml.BeanDefinitionParser} that parses an {@code initialize-keyspace} element
 * and creates a {@link BeanDefinition} of type {@link SessionFactoryInitializer}. Picks up nested {@code script}
 * elements and configures a {@link ResourceKeyspacePopulator} for them.
 *
 * @author Mark Paluch
 * @since 3.0
 */
class InitializeKeyspaceBeanDefinitionParser extends AbstractBeanDefinitionParser {

	@Override
	protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(SessionFactoryInitializer.class);

		if (element.hasAttribute("session-factory-ref")) {
			addRequiredPropertyReference(builder, "session", element, "session-factory-ref");
		}

		builder.addPropertyValue("enabled", element.getAttribute("enabled"));

		parseKeyspacePopulator(element, builder);

		builder.getRawBeanDefinition().setSource(element);

		return builder.getBeanDefinition();
	}

	@Override
	protected boolean shouldGenerateId() {
		return true;
	}

	public static void parseKeyspacePopulator(Element element, BeanDefinitionBuilder builder) {

		List<Element> scripts = DomUtils.getChildElementsByTagName(element, "script");

		if (!scripts.isEmpty()) {
			builder.addPropertyValue("keyspacePopulator", createKeyspacePopulator(element, scripts, "INIT"));
			builder.addPropertyValue("keyspaceCleaner", createKeyspacePopulator(element, scripts, "DESTROY"));
		}
	}

	private static BeanDefinition createKeyspacePopulator(Element element, List<Element> scripts, String execution) {

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(CompositeKeyspacePopulator.class);

		boolean ignoreFailedDrops = element.getAttribute("ignore-failures").equals("DROPS");
		boolean continueOnError = element.getAttribute("ignore-failures").equals("ALL");

		ManagedList<BeanMetadataElement> delegates = new ManagedList<>();
		for (Element scriptElement : scripts) {

			String executionAttr = scriptElement.getAttribute("execution");

			if (!StringUtils.hasText(executionAttr)) {
				executionAttr = "INIT";
			}
			if (!execution.equals(executionAttr)) {
				continue;
			}

			BeanDefinitionBuilder delegate = BeanDefinitionBuilder.genericBeanDefinition(ResourceKeyspacePopulator.class);
			delegate.addPropertyValue("ignoreFailedDrops", ignoreFailedDrops);
			delegate.addPropertyValue("continueOnError", continueOnError);

			// Use a factory bean for the resources so they can be given an order if a pattern is used
			BeanDefinitionBuilder resourcesFactory = BeanDefinitionBuilder
					.genericBeanDefinition(SortedResourcesFactoryBean.class);
			resourcesFactory.addConstructorArgValue(new TypedStringValue(scriptElement.getAttribute("location")));
			delegate.addPropertyValue("scripts", resourcesFactory.getBeanDefinition());

			if (StringUtils.hasLength(scriptElement.getAttribute("encoding"))) {
				delegate.addPropertyValue("cqlScriptEncoding", new TypedStringValue(scriptElement.getAttribute("encoding")));
			}

			String separator = getSeparator(element, scriptElement);
			if (separator != null) {
				delegate.addPropertyValue("separator", new TypedStringValue(separator));
			}
			delegates.add(delegate.getBeanDefinition());
		}
		builder.addPropertyValue("populators", delegates);

		return builder.getBeanDefinition();
	}

	@Nullable
	private static String getSeparator(Element element, Element scriptElement) {

		String scriptSeparator = scriptElement.getAttribute("separator");
		if (StringUtils.hasLength(scriptSeparator)) {
			return scriptSeparator;
		}

		String elementSeparator = element.getAttribute("separator");
		if (StringUtils.hasLength(elementSeparator)) {
			return elementSeparator;
		}

		return null;
	}
}
