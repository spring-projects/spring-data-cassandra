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
package org.springframework.cassandra.config.xml;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSimpleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.cassandra.config.CassandraSessionFactoryBean;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Parser for &lt;session&gt; definitions.
 * 
 * @author David Webb
 * @author Matthew T. Adams
 */

public class CassandraSessionParser extends AbstractSimpleBeanDefinitionParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return CassandraSessionFactoryBean.class;
	}

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {

		String id = super.resolveId(element, definition, parserContext);
		return StringUtils.hasText(id) ? id : BeanNames.CASSANDRA_SESSION;
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		parseKeyspaceName(element, builder);
		parseClusterRef(element, builder);
		parseScripts(element, builder, "startup-cql", "startupScripts");
		parseScripts(element, builder, "shutdown-cql", "shutdownScripts");
	}

	protected void parseScripts(Element element, BeanDefinitionBuilder builder, String elementName, String propertyName) {

		List<String> scripts = parseScripts(element, elementName);
		builder.addPropertyValue(propertyName, scripts);
	}

	protected void parseClusterRef(Element element, BeanDefinitionBuilder builder) {

		String clusterRef = element.getAttribute("cluster-ref");
		if (!StringUtils.hasText(clusterRef)) {
			clusterRef = BeanNames.CASSANDRA_CLUSTER;
		}
		builder.addPropertyReference("cluster", clusterRef);
	}

	protected void parseKeyspaceName(Element element, BeanDefinitionBuilder builder) {

		String keyspaceName = element.getAttribute("keyspace-name");
		if (!StringUtils.hasText(keyspaceName)) {
			keyspaceName = null;
		}
		builder.addPropertyValue("keyspaceName", keyspaceName);
	}

	protected List<String> parseScripts(Element element, String elementName) {

		NodeList nodes = element.getElementsByTagName(elementName);
		int length = nodes.getLength();
		List<String> scripts = new ArrayList<String>(length);

		for (int i = 0; i < length; i++) {
			Element script = (Element) nodes.item(i);
			scripts.add(DomUtils.getTextValue(script));
		}

		return scripts;
	}
}
