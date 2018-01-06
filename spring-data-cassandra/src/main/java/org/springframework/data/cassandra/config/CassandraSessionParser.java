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

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;

/**
 * Spring Data Cassandra XML namespace parser for the {@code cassandra:session} element.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
class CassandraSessionParser extends CassandraCqlSessionParser {

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.config.xml.CassandraCqlSessionParser#getBeanClass(org.w3c.dom.Element)
	 */
	@Override
	protected Class<?> getBeanClass(Element element) {
		return CassandraSessionFactoryBean.class;
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.config.xml.CassandraCqlSessionParser#doParse(org.w3c.dom.Element, org.springframework.beans.factory.xml.ParserContext, org.springframework.beans.factory.support.BeanDefinitionBuilder)
	 */
	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		super.doParse(element, parserContext, builder);

		CassandraMappingXmlBeanFactoryPostProcessorRegistrar.ensureRegistration(element, parserContext);
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.config.xml.CassandraCqlSessionParser#parseUnhandledSessionElementAttribute(org.w3c.dom.Attr, org.springframework.beans.factory.xml.ParserContext, org.springframework.beans.factory.support.BeanDefinitionBuilder)
	 */
	@Override
	protected void parseUnhandledSessionElementAttribute(Attr attribute, ParserContext parserContext,
			BeanDefinitionBuilder builder) {

		String name = attribute.getName();

		if ("cassandra-converter-ref".equals(name)) {
			addOptionalPropertyReference(builder, "converter", attribute, DefaultBeanNames.CONVERTER);
		} else if ("schema-action".equals(name)) {
			addOptionalPropertyValue(builder, "schemaAction", attribute, SchemaAction.NONE.name());
		} else {
			super.parseUnhandledSessionElementAttribute(attribute, parserContext, builder);
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.config.xml.CassandraCqlSessionParser#setDefaultProperties(org.springframework.beans.factory.support.BeanDefinitionBuilder)
	 */
	@Override
	protected void setDefaultProperties(BeanDefinitionBuilder builder) {

		super.setDefaultProperties(builder);

		addRequiredPropertyValue(builder, "schemaAction", SchemaAction.NONE.name());
		addRequiredPropertyReference(builder, "converter", DefaultBeanNames.CONVERTER);
	}
}
