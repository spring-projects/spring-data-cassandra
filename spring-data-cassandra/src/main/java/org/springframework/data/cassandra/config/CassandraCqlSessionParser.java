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

import static org.springframework.data.cassandra.config.ParsingUtils.*;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;

/**
 * Parser for &lt;session&gt; definitions.
 *
 * @author David Webb
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
class CassandraCqlSessionParser extends AbstractSingleBeanDefinitionParser {

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser#getBeanClass(org.w3c.dom.Element)
	 */
	@Override
	protected Class<?> getBeanClass(Element element) {
		return CassandraCqlSessionFactoryBean.class;
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.xml.AbstractBeanDefinitionParser#resolveId(org.w3c.dom.Element, org.springframework.beans.factory.support.AbstractBeanDefinition, org.springframework.beans.factory.xml.ParserContext)
	 */
	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {

		String id = super.resolveId(element, definition, parserContext);

		return StringUtils.hasText(id) ? id : DefaultCqlBeanNames.SESSION;
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser#doParse(org.w3c.dom.Element, org.springframework.beans.factory.xml.ParserContext, org.springframework.beans.factory.support.BeanDefinitionBuilder)
	 */
	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		setDefaultProperties(builder);

		parseSessionAttributes(element, parserContext, builder);
		parseSessionChildElements(element, parserContext, builder);
	}

	protected void setDefaultProperties(BeanDefinitionBuilder builder) {
		addRequiredPropertyReference(builder, "cluster", DefaultCqlBeanNames.CLUSTER);
	}

	private void parseSessionAttributes(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		NamedNodeMap attributes = element.getAttributes();
		int length = attributes.getLength();

		for (int i = 0; i < length; i++) {

			Attr attribute = (Attr) attributes.item(i);
			if ("id".equals(attribute.getName())) {
				continue;
			}

			String name = attribute.getName();

			if ("keyspace-name".equals(name)) {
				addRequiredPropertyValue(builder, "keyspaceName", attribute);
			} else if ("cluster-ref".equals(name)) {
				addOptionalPropertyReference(builder, "cluster", attribute, DefaultCqlBeanNames.CLUSTER);
			} else {
				parseUnhandledSessionElementAttribute(attribute, parserContext, builder);
			}
		}
	}

	private void parseSessionChildElements(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		for (Element child : DomUtils.getChildElements(element)) {

			if ("startup-cql".equals(child.getLocalName())) {
				builder.addPropertyValue("startupScripts", DomUtils.getTextValue(child));
			} else if ("shutdown-cql".equals(child.getLocalName())) {
				builder.addPropertyValue("shutdownScripts", DomUtils.getTextValue(child));
			} else {
				throw new IllegalStateException(String.format("encountered unhandled element [%s]", child.getLocalName()));
			}
		}
	}

	/**
	 * Parse the given session element attribute. This method is intended to be overridden by subclasses so that any
	 * attributes not known to this class can be properly parsed. The default implementation throws
	 * {@link IllegalStateException}.
	 */
	protected void parseUnhandledSessionElementAttribute(Attr attribute, ParserContext parserContext,
			BeanDefinitionBuilder builder) {

		throw new IllegalStateException(
				String.format("encountered unhandled session element attribute [%s]", attribute.getName()));
	}
}
