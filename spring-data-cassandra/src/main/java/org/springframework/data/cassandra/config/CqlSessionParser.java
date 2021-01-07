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

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedSet;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceAttributes;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;

import org.w3c.dom.Element;

/**
 * Parser for &lt;session&gt; definitions.
 *
 * @author Mark Paluch
 */
class CqlSessionParser extends AbstractSingleBeanDefinitionParser {

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser#getBeanClass(org.w3c.dom.Element)
	 */
	@Override
	protected Class<?> getBeanClass(Element element) {
		return ExtendedCqlSessionFactoryBean.class;
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.xml.AbstractBeanDefinitionParser#resolveId(org.w3c.dom.Element, org.springframework.beans.factory.support.AbstractBeanDefinition, org.springframework.beans.factory.xml.ParserContext)
	 */
	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {

		String id = super.resolveId(element, definition, parserContext);

		return StringUtils.hasText(id) ? id : DefaultCqlBeanNames.SESSION;
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser#doParse(org.w3c.dom.Element, org.springframework.beans.factory.xml.ParserContext, org.springframework.beans.factory.support.BeanDefinitionBuilder)
	 */
	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		addOptionalPropertyValue(builder, "keyspaceName", element, "keyspace-name");
		addOptionalPropertyValue(builder, "contactPointsAsString", element, "contact-points");
		addOptionalPropertyValue(builder, "localDatacenter", element, "local-datacenter");
		addOptionalPropertyValue(builder, "password", element, "password");
		addOptionalPropertyValue(builder, "port", element, "port");
		addOptionalPropertyValue(builder, "username", element, "username");

		if (element.hasAttribute("cassandra-converter-ref")) {
			addRequiredPropertyReference(builder, "converter", element, "cassandra-converter-ref");
		}

		addOptionalPropertyValue(builder, "schemaAction", element, "schema-action", SchemaAction.NONE.name());

		parseChildElements(element, parserContext, builder);

		builder.getRawBeanDefinition().setSource(element);
	}

	/**
	 * Parses child elements of cluster.
	 *
	 * @param element {@link Element} to parse.
	 * @param parserContext XML parser context and state.
	 * @param builder parent {@link BeanDefinitionBuilder}.
	 */
	protected void parseChildElements(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		ManagedSet<BeanDefinition> keyspaceActionSpecificationBeanDefinitions = new ManagedSet<>();

		List<String> startupScripts = new ArrayList<>();
		List<String> shutdownScripts = new ArrayList<>();

		// parse child elements
		for (Element subElement : DomUtils.getChildElements(element)) {

			String name = subElement.getLocalName();

			if ("keyspace".equals(name)) {
				keyspaceActionSpecificationBeanDefinitions
						.add(newKeyspaceActionSpecificationBeanDefinition(subElement, parserContext));
			} else if ("keyspace-startup-cql".equals(name)) {
				startupScripts.add(parseScript(subElement));
			} else if ("keyspace-shutdown-cql".equals(name)) {
				shutdownScripts.add(parseScript(subElement));
			} else if ("startup-cql".equals(name)) {
				startupScripts.add(parseScript(subElement));
			} else if ("shutdown-cql".equals(name)) {
				shutdownScripts.add(parseScript(subElement));
			} else {
				throw new IllegalStateException(String.format("encountered unhandled element [%s]", subElement.getLocalName()));
			}
		}

		builder.addPropertyValue("keyspaceActions", keyspaceActionSpecificationBeanDefinitions);
		builder.addPropertyValue("startupScripts", startupScripts);
		builder.addPropertyValue("shutdownScripts", shutdownScripts);
	}

	/**
	 * Returns a {@link BeanDefinition} for a {@link KeyspaceActionSpecification} object.
	 *
	 * @param element Element being parsed.
	 * @param parserContext XML parser context and state.
	 * @return the {@link BeanDefinition} or {@literal null} if action is not given.
	 */
	private BeanDefinition newKeyspaceActionSpecificationBeanDefinition(Element element, ParserContext parserContext) {

		BeanDefinitionBuilder builder = BeanDefinitionBuilder
				.genericBeanDefinition(KeyspaceActionSpecificationFactoryBean.class);

		// add required replication defaults
		addRequiredPropertyValue(builder, "replicationStrategy", KeyspaceAttributes.DEFAULT_REPLICATION_STRATEGY.name());

		addRequiredPropertyValue(builder, "replicationFactor",
				String.valueOf(KeyspaceAttributes.DEFAULT_REPLICATION_FACTOR));

		addRequiredPropertyValue(builder, "name", element, "name");
		addOptionalPropertyValue(builder, "durableWrites", element, "durable-writes", "false");
		addRequiredPropertyValue(builder, "action", element, "action");

		return getSourceBeanDefinition(builder, parserContext, element);
	}

	/**
	 * Parse CQL script {@link Element}s.
	 *
	 * @param element {@link Element} to parse.
	 * @return return the contents of the {@link Element}, which should contain the CQL script.
	 */
	String parseScript(Element element) {
		return element.getTextContent();
	}

	/**
	 * Wrapper to enable setting contact points as string to avoid over loaded setContactPoints reflection confusion that
	 * depends on the reflection method load order.
	 */
	static class ExtendedCqlSessionFactoryBean extends CqlSessionFactoryBean {

		/**
		 * Bridge method for {@link #setContactPoints(String)}.
		 *
		 * @param contactPoints
		 */
		public void setContactPointsAsString(String contactPoints) {
			setContactPoints(contactPoints);
		}
	}
}
