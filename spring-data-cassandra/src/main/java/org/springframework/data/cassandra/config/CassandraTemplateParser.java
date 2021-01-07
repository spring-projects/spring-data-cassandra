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

import static org.springframework.data.cassandra.config.ParsingUtils.*;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * Spring Data Cassandra XML namespace parser for the {@code cassandra:template} element.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @author Mateusz Szymczak
 */
class CassandraTemplateParser extends AbstractSingleBeanDefinitionParser {

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.config.xml.CassandraCqlTemplateParser#getBeanClass(org.w3c.dom.Element)
	 */
	@Override
	protected Class<?> getBeanClass(Element element) {
		return CassandraTemplateFactoryBean.class;
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.config.xml.CassandraCqlTemplateParser#resolveId(org.w3c.dom.Element, org.springframework.beans.factory.support.AbstractBeanDefinition, org.springframework.beans.factory.xml.ParserContext)
	 */
	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {

		String id = super.resolveId(element, definition, parserContext);
		return StringUtils.hasText(id) ? id : DefaultBeanNames.DATA_TEMPLATE;
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.config.xml.CassandraCqlTemplateParser#doParse(org.w3c.dom.Element, org.springframework.beans.factory.xml.ParserContext, org.springframework.beans.factory.support.BeanDefinitionBuilder)
	 */
	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		super.doParse(element, parserContext, builder);

		if (element.hasAttribute("cql-template-ref")) {
			addRequiredPropertyReference(builder, "cqlOperations", element, "cql-template-ref");
		} else if (element.hasAttribute("session-factory-ref")) {
			addRequiredPropertyReference(builder, "sessionFactory", element, "session-factory-ref");
		} else {
			addOptionalPropertyReference(builder, "session", element, "session-ref", DefaultBeanNames.SESSION);
		}

		if (element.hasAttribute("cassandra-converter-ref")) {
			addRequiredPropertyReference(builder, "converter", element, "cassandra-converter-ref");
		}

		builder.getRawBeanDefinition().setSource(element);
	}
}
