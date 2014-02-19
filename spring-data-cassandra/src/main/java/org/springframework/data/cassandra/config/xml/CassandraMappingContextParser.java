/*
 * Copyright 2013-2014 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.config.xml;

import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.cassandra.config.DefaultDataBeanNames;
import org.springframework.data.cassandra.mapping.DefaultCassandraMappingContext;
import org.springframework.data.cassandra.mapping.EntityMapping;
import org.springframework.data.cassandra.mapping.Mapping;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * Spring Data Cassandra XML namespace parser for the &lt;mapping&gt; element.
 * 
 * @author Matthew T. Adams
 */
public class CassandraMappingContextParser extends AbstractSingleBeanDefinitionParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return DefaultCassandraMappingContext.class;
	}

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {

		String id = super.resolveId(element, definition, parserContext);
		return StringUtils.hasText(id) ? id : DefaultDataBeanNames.CONTEXT;
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		CassandraMappingXmlBeanFactoryPostProcessorRegistrar.ensureRegistration(element, parserContext);

		parseMapping(element, builder);
	}

	protected void parseMapping(Element element, BeanDefinitionBuilder builder) {

		Set<EntityMapping> mappings = new HashSet<EntityMapping>();

		for (Element entity : DomUtils.getChildElementsByTagName(element, "entity")) {

			EntityMapping entityMapping = parseEntity(entity);

			if (entityMapping != null) {
				mappings.add(entityMapping);
			}
		}

		Mapping mapping = new Mapping();
		mapping.setEntityMappings(mappings);

		builder.addPropertyValue("mapping", mapping);
	}

	protected EntityMapping parseEntity(Element entity) {

		String className = entity.getAttribute("class");
		if (!StringUtils.hasText(className)) {
			throw new IllegalStateException("class attribute must not be empty");
		}

		Element table = DomUtils.getChildElementByTagName(entity, "table");
		if (table == null) {
			return null;
		}

		String tableName = table.getAttribute("name");
		if (!StringUtils.hasText(tableName)) {
			tableName = "";
		}

		String forceQuote = table.getAttribute("force-quote");
		if (!StringUtils.hasText(forceQuote)) {
			forceQuote = Boolean.FALSE.toString();
		}

		// TODO: parse future entity mappings here, like table options

		return new EntityMapping(className, tableName, forceQuote);
	}
}
