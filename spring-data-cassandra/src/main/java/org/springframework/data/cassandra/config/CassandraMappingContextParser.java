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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.EntityMapping;
import org.springframework.data.cassandra.core.mapping.Mapping;
import org.springframework.data.cassandra.core.mapping.PropertyMapping;
import org.springframework.data.cassandra.core.mapping.SimpleUserTypeResolver;
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

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser#getBeanClass(org.w3c.dom.Element)
	 */
	@Override
	protected Class<?> getBeanClass(Element element) {
		return CassandraMappingContext.class;
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.xml.AbstractBeanDefinitionParser#resolveId(org.w3c.dom.Element, org.springframework.beans.factory.support.AbstractBeanDefinition, org.springframework.beans.factory.xml.ParserContext)
	 */
	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {

		String id = super.resolveId(element, definition, parserContext);

		return (StringUtils.hasText(id) ? id : DefaultBeanNames.CONTEXT);
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser#doParse(org.w3c.dom.Element, org.springframework.beans.factory.xml.ParserContext, org.springframework.beans.factory.support.BeanDefinitionBuilder)
	 */
	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		parseMapping(element, builder);
		builder.getRawBeanDefinition().setSource(element);
	}

	private void parseMapping(Element element, BeanDefinitionBuilder builder) {

		String packages = element.getAttribute("entity-base-packages");

		if (StringUtils.hasText(packages)) {
			try {
				Set<Class<?>> entityClasses = CassandraEntityClassScanner
						.scan(StringUtils.commaDelimitedListToStringArray(packages));

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
			Assert.isTrue(userTypeResolvers.isEmpty(), "Must not define user-type-resolver and user-type-resolver-ref");
			builder.addPropertyReference("userTypeResolver", userTypeResolverRef);
		}

		if (!userTypeResolvers.isEmpty()) {
			BeanDefinition userTypeResolver = parseUserTypeResolver(userTypeResolvers.get(0));
			builder.addPropertyValue("userTypeResolver", userTypeResolver);
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
		String forceQuote = "";

		if (table != null) {
			tableName = table.getAttribute("name");
			tableName = (StringUtils.hasText(tableName) ? tableName : "");
			forceQuote = String.valueOf(Boolean.parseBoolean(table.getAttribute("force-quote")));
		}

		Map<String, PropertyMapping> propertyMappings = parsePropertyMappings(entity);

		EntityMapping entityMapping = new EntityMapping(className, tableName, forceQuote);
		entityMapping.setPropertyMappings(propertyMappings);

		return entityMapping;
	}

	private BeanDefinition parseUserTypeResolver(Element entity) {

		String sessionRef = entity.getAttribute("session-ref");

		if (StringUtils.hasText(sessionRef)) {
			BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(SimpleUserTypeResolver.class);
			builder.addConstructorArgReference(sessionRef);
			return builder.getBeanDefinition();
		}

		String keyspaceName = entity.getAttribute("keyspace-name");
		Assert.state(StringUtils.hasText(keyspaceName), "keyspace-name attribute must not be null or empty");
		String clusterRef = entity.getAttribute("cluster-ref");

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(SimpleUserTypeResolver.class);
		builder.addConstructorArgReference(StringUtils.hasText(clusterRef) ? clusterRef : DefaultCqlBeanNames.CLUSTER);
		builder.addConstructorArgValue(keyspaceName);

		return builder.getBeanDefinition();
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
				propertyMapping.setForceQuote(value);
			}

			propertyMappings.put(propertyMapping.getPropertyName(), propertyMapping);
		}

		return propertyMappings;
	}
}
