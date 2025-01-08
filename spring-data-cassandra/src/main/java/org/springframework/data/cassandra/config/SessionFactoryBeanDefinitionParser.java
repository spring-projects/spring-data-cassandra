/*
 * Copyright 2013-2025 the original author or authors.
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
 * Parser for &lt;session-factory&gt; definitions.
 *
 * @author Mark Paluch
 */
class SessionFactoryBeanDefinitionParser extends AbstractSingleBeanDefinitionParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return SessionFactoryFactoryBean.class;
	}

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {

		String id = super.resolveId(element, definition, parserContext);

		return StringUtils.hasText(id) ? id : DefaultBeanNames.SESSION_FACTORY;
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		addOptionalPropertyReference(builder, "session", element, "session-ref", DefaultBeanNames.SESSION);
		addOptionalPropertyReference(builder, "converter", element, "cassandra-converter-ref", DefaultBeanNames.CONVERTER);
		addOptionalPropertyValue(builder, "schemaAction", element, "schema-action", SchemaAction.NONE.name());

		InitializeKeyspaceBeanDefinitionParser.parseKeyspacePopulator(element, builder);
		builder.getRawBeanDefinition().setSource(element);
	}
}
