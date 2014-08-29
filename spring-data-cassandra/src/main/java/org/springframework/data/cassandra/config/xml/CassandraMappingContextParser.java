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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.cassandra.config.CassandraEntityClassScanner;
import org.springframework.data.cassandra.config.DefaultBeanNames;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.mapping.EntityMapping;
import org.springframework.data.cassandra.mapping.Mapping;
import org.springframework.data.cassandra.mapping.PropertyMapping;
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
		return BasicCassandraMappingContext.class;
	}

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {

		String id = super.resolveId(element, definition, parserContext);
		return StringUtils.hasText(id) ? id : DefaultBeanNames.CONTEXT;
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		CassandraMappingXmlBeanFactoryPostProcessorRegistrar.ensureRegistration(element, parserContext);

		parseMapping(element, builder);
	}

	protected void parseMapping(Element element, BeanDefinitionBuilder builder) {

		String packages = element.getAttribute("entity-base-packages");
		if (StringUtils.hasText(packages)) {
			try {
				Set<Class<?>> entityClasses = CassandraEntityClassScanner.scan(StringUtils
						.commaDelimitedListToStringArray(packages));
				builder.addPropertyValue("initialEntitySet", entityClasses);
			} catch (Exception x) {
				throw new IllegalArgumentException(String.format(
						"encountered exception while scanning for entity classes in package(s) [%s]", packages), x);
			}
		}

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

		String tableName = "";
		String forceQuote = "";
		Element table = DomUtils.getChildElementByTagName(entity, "table");
		if (table != null) {
			tableName = table.getAttribute("name");
			if (!StringUtils.hasText(tableName)) {
				tableName = "";
			}

			forceQuote = table.getAttribute("force-quote");
			if (!StringUtils.hasText(forceQuote)) {
				forceQuote = Boolean.FALSE.toString();
			}
		}

		// TODO: parse future entity mappings here, like table options

		Map<String, PropertyMapping> propertyMappings = parsePropertyMappings(entity);

		EntityMapping entityMapping = new EntityMapping(className, tableName, forceQuote);
		entityMapping.setPropertyMappings(propertyMappings);

		return entityMapping;
	}

	protected Map<String, PropertyMapping> parsePropertyMappings(Element entity) {

		Map<String, PropertyMapping> pms = new HashMap<String, PropertyMapping>();

		for (Element property : DomUtils.getChildElementsByTagName(entity, "property")) {

			String value = property.getAttribute("name");
			if (!StringUtils.hasText(value)) {
				throw new IllegalStateException("name attribute must not be empty");
			}
			PropertyMapping pm = new PropertyMapping(value);

			value = property.getAttribute("column-name");
			if (StringUtils.hasText(value)) {
				pm.setColumnName(value);
			}

			value = property.getAttribute("force-quote");
			if (StringUtils.hasText(value)) {
				pm.setForceQuote(value);
			}

			pms.put(pm.getPropertyName(), pm);
		}

		return pms;
	}
}
