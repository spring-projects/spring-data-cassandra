/*
 * Copyright 2013-present the original author or authors.
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.EntityMapping;
import org.springframework.data.cassandra.core.mapping.Mapping;
import org.springframework.data.cassandra.core.mapping.PropertyMapping;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;

import org.w3c.dom.Element;

/**
 * Spring Data Cassandra XML namespace parser for the {@code cassandra:mapping} element.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
class CassandraMappingContextParser extends AbstractSingleBeanDefinitionParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return CassandraMappingContext.class;
	}

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {

		String id = super.resolveId(element, definition, parserContext);

		return (StringUtils.hasText(id) ? id : DefaultBeanNames.CONTEXT);
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		parseMapping(element, builder, parserContext.getReaderContext().getBeanClassLoader());
		builder.getRawBeanDefinition().setSource(element);
	}

	private void parseMapping(Element element, BeanDefinitionBuilder builder, @Nullable ClassLoader classLoader) {

		String packages = element.getAttribute("entity-base-packages");

		if (StringUtils.hasText(packages)) {
			try {
				CassandraEntityClassScanner scanner = new CassandraEntityClassScanner();
				scanner.setBeanClassLoader(classLoader);
				scanner.setEntityBasePackages(StringUtils.commaDelimitedListToSet(packages));

				Set<Class<?>> entityClasses = scanner.scanForEntityClasses();

				builder.addPropertyValue("initialEntitySet", entityClasses);
			} catch (Exception x) {
				throw new IllegalArgumentException(
						String.format("encountered exception while scanning for entity classes in package(s) [%s]", packages), x);
			}
		}

		Set<EntityMapping> mappings = new HashSet<>();

		DomUtils.getChildElementsByTagName(element, "entity").forEach(entity -> {

			EntityMapping entityMapping = parseEntity(entity);
			mappings.add(entityMapping);
		});

		List<Element> userTypeResolvers = DomUtils.getChildElementsByTagName(element, "user-type-resolver");
		String userTypeResolverRef = element.getAttribute("user-type-resolver-ref");

		if (StringUtils.hasText(userTypeResolverRef)) {
			throw new IllegalArgumentException(
					"user-type-resolver-ref attribute is no longer supported. Please configure the user-type-resolver using the Converter.");
		}

		if (!userTypeResolvers.isEmpty()) {
			throw new IllegalArgumentException(
					"user-type-resolver is no longer supported. Please configure the user-type-resolver using the Converter.");
		}

		Mapping mapping = new Mapping();
		mapping.setEntityMappings(mappings);

		builder.addPropertyValue("mapping", mapping);
	}

	private EntityMapping parseEntity(Element entity) {

		String className = entity.getAttribute("class");

		Assert.state(StringUtils.hasText(className), "class attribute must not be empty");

		Element table = DomUtils.getChildElementByTagName(entity, "table");

		String tableName = "";

		if (table != null) {
			tableName = table.getAttribute("name");
			tableName = (StringUtils.hasText(tableName) ? tableName : "");
		}

		Map<String, PropertyMapping> propertyMappings = parsePropertyMappings(entity);

		EntityMapping entityMapping = new EntityMapping(className, tableName);
		entityMapping.setPropertyMappings(propertyMappings);

		return entityMapping;
	}

	private Map<String, PropertyMapping> parsePropertyMappings(Element entity) {

		Map<String, PropertyMapping> propertyMappings = new HashMap<>();

		for (Element property : DomUtils.getChildElementsByTagName(entity, "property")) {

			String value = property.getAttribute("name");

			Assert.state(StringUtils.hasText(value), "name attribute must not be empty");

			PropertyMapping propertyMapping = new PropertyMapping(value);

			value = property.getAttribute("column-name");

			if (StringUtils.hasText(value)) {
				propertyMapping.setColumnName(value);
			}

			value = property.getAttribute("force-quote");

			if (StringUtils.hasText(value)) {
				throw new IllegalArgumentException("force-quote is no longer supported.");
			}

			propertyMappings.put(propertyMapping.getPropertyName(), propertyMapping);
		}

		return propertyMappings;
	}

}
