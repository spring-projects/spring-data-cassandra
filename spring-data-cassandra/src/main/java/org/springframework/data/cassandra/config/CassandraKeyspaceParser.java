/*
 * Copyright 2011-2012 the original author or authors.
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

import java.util.List;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.xml.AbstractSimpleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.cassandra.config.KeyspaceAttributes;
import org.springframework.cassandra.config.xml.BeanNames;
import org.springframework.data.config.ParsingUtils;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * Parser for &lt;keyspace;gt; definitions.
 * 
 * @author Alex Shvid
 */

public class CassandraKeyspaceParser extends AbstractSimpleBeanDefinitionParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return CassandraKeyspaceFactoryBean.class;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.xml.AbstractBeanDefinitionParser#resolveId(org.w3c.dom.Element, org.springframework.beans.factory.support.AbstractBeanDefinition, org.springframework.beans.factory.xml.ParserContext)
	 */
	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {

		String id = super.resolveId(element, definition, parserContext);
		return StringUtils.hasText(id) ? id : BeanNames.CASSANDRA_KEYSPACE;
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		String name = element.getAttribute("name");
		if (StringUtils.hasText(name)) {
			builder.addPropertyValue("keyspace", name);
		}

		String clusterRef = element.getAttribute("cassandra-cluster-ref");
		if (!StringUtils.hasText(clusterRef)) {
			clusterRef = BeanNames.CASSANDRA_CLUSTER;
		}
		builder.addPropertyReference("cluster", clusterRef);

		String converterRef = element.getAttribute("cassandra-converter-ref");
		if (StringUtils.hasText(converterRef)) {
			builder.addPropertyReference("converter", converterRef);
		}

		postProcess(builder, element);
	}

	@Override
	protected void postProcess(BeanDefinitionBuilder builder, Element element) {
		List<Element> subElements = DomUtils.getChildElements(element);

		// parse nested elements
		for (Element subElement : subElements) {
			String name = subElement.getLocalName();

			if ("keyspace-attributes".equals(name)) {
				builder.addPropertyValue("keyspaceAttributes", parseKeyspaceAttributes(subElement));
			}
		}

	}

	private BeanDefinition parseKeyspaceAttributes(Element element) {
		BeanDefinitionBuilder defBuilder = BeanDefinitionBuilder.genericBeanDefinition(KeyspaceAttributes.class);
		ParsingUtils.setPropertyValue(defBuilder, element, "auto", "auto");
		ParsingUtils.setPropertyValue(defBuilder, element, "replication-strategy", "replicationStrategy");
		ParsingUtils.setPropertyValue(defBuilder, element, "replication-factor", "replicationFactor");
		ParsingUtils.setPropertyValue(defBuilder, element, "durable-writes", "durableWrites");

		List<Element> subElements = DomUtils.getChildElements(element);
		ManagedList<Object> tables = new ManagedList<Object>(subElements.size());

		// parse nested elements
		for (Element subElement : subElements) {
			String name = subElement.getLocalName();

			if ("table".equals(name)) {
				tables.add(parseTable(subElement));
			}
		}
		if (!tables.isEmpty()) {
			defBuilder.addPropertyValue("tables", tables);
		}

		return defBuilder.getBeanDefinition();
	}

	private BeanDefinition parseTable(Element element) {
		BeanDefinitionBuilder defBuilder = BeanDefinitionBuilder.genericBeanDefinition(TableAttributes.class);
		ParsingUtils.setPropertyValue(defBuilder, element, "entity", "entity");
		ParsingUtils.setPropertyValue(defBuilder, element, "name", "name");
		return defBuilder.getBeanDefinition();
	}

}
