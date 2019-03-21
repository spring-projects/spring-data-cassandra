/*
 * Copyright 2013-2014 the original author or authors
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
package org.springframework.data.cassandra.config.xml;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.cassandra.config.xml.CassandraCqlTemplateParser;
import org.springframework.data.cassandra.config.DefaultBeanNames;
import org.springframework.data.cassandra.config.CassandraTemplateFactoryBean;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * Spring Data Cassandra XML namespace parser for the &lt;template&gt; element.
 * 
 * @author Matthew T. Adams
 */
public class CassandraTemplateParser extends CassandraCqlTemplateParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return CassandraTemplateFactoryBean.class;
	}

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {

		String id = super.resolveId(element, definition, parserContext);
		return StringUtils.hasText(id) ? id : DefaultBeanNames.DATA_TEMPLATE;
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		CassandraMappingXmlBeanFactoryPostProcessorRegistrar.ensureRegistration(element, parserContext);

		super.doParse(element, parserContext, builder);

		parseConverterAttribute(element, parserContext, builder);
	}

	protected void parseConverterAttribute(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		String converterRef = element.getAttribute("cassandra-converter-ref");
		if (!StringUtils.hasText(converterRef)) {
			converterRef = DefaultBeanNames.CONVERTER;
		}
		builder.addPropertyReference("converter", converterRef);
	}
}
