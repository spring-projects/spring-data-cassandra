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

import static org.springframework.data.config.ParsingUtils.getSourceBeanDefinition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedSet;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.cassandra.config.CassandraClusterFactoryBean;
import org.springframework.cassandra.config.KeyspaceActionSpecificationFactoryBean;
import org.springframework.cassandra.config.KeyspaceAttributes;
import org.springframework.cassandra.config.MultiLevelSetFlattenerFactoryBean;
import org.springframework.cassandra.config.PoolingOptionsConfig;
import org.springframework.cassandra.config.SocketOptionsConfig;
import org.springframework.cassandra.core.keyspace.DefaultOption;
import org.springframework.cassandra.core.keyspace.KeyspaceOption.ReplicationStrategy;
import org.springframework.cassandra.core.keyspace.Option;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

import com.datastax.driver.core.AuthProvider;

/**
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author David Webb
 */
public class CassandraClusterParser extends AbstractBeanDefinitionParser {

	private final static Logger log = LoggerFactory.getLogger(CassandraClusterParser.class);

	// @Override
	// protected Class<?> getBeanClass(Element element) {
	// return CassandraClusterFactoryBean.class;
	// }

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {

		String id = super.resolveId(element, definition, parserContext);
		return StringUtils.hasText(id) ? id : BeanNames.CASSANDRA_CLUSTER;
	}

	@Override
	protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(CassandraClusterFactoryBean.class);
		builder.getRawBeanDefinition().setSource(parserContext.extractSource(element));
		builder.getRawBeanDefinition().setDestroyMethodName("destroy");
		if (parserContext.isNested()) {
			// Inner bean definition must receive same scope as containing bean.
			builder.setScope(parserContext.getContainingBeanDefinition().getScope());
		}

		if (parserContext.isDefaultLazyInit()) {
			// Default-lazy-init applies to custom bean definitions as well.
			builder.setLazyInit(true);
		}

		doParse(element, parserContext, builder);

		return builder.getBeanDefinition();
	}

	protected void doParse(Element element, ParserContext context, BeanDefinitionBuilder builder) {

		String contactPoints = element.getAttribute("contactPoints");
		if (StringUtils.hasText(contactPoints)) {
			builder.addPropertyValue("contactPoints", contactPoints);
		}

		String port = element.getAttribute("port");
		if (StringUtils.hasText(port)) {
			builder.addPropertyValue("port", port);
		}

		String compression = element.getAttribute("compression");
		if (StringUtils.hasText(compression)) {
			builder.addPropertyValue("compressionType", compression);
		}

		String authProvider = element.getAttribute("auth-info-provider-ref");
		if (StringUtils.hasText(authProvider)) {
			log.info(authProvider);
			builder.addPropertyReference("authProvider", authProvider);
		}

		parseChildElements(element, context, builder);
	}

	protected void parseChildElements(Element element, ParserContext context, BeanDefinitionBuilder builder) {

		ManagedSet<BeanDefinition> keyspaceActionSpecificationBeanDefinitions = new ManagedSet<BeanDefinition>();
		List<String> startupScripts = new ArrayList<String>();
		List<String> shutdownScripts = new ArrayList<String>();

		List<Element> elements = DomUtils.getChildElements(element);
		BeanDefinition keyspaceActionSpecificationBeanDefinition = null;

		// parse nested elements
		for (Element subElement : elements) {

			String name = subElement.getLocalName();

			if ("local-pooling-options".equals(name)) {
				builder.addPropertyValue("localPoolingOptions", parsePoolingOptions(subElement));
			} else if ("remote-pooling-options".equals(name)) {
				builder.addPropertyValue("remotePoolingOptions", parsePoolingOptions(subElement));
			} else if ("socket-options".equals(name)) {
				builder.addPropertyValue("socketOptions", parseSocketOptions(subElement));
			} else if ("keyspace".equals(name)) {

				keyspaceActionSpecificationBeanDefinition = getKeyspaceSpecificationBeanDefinition(subElement, context);
				keyspaceActionSpecificationBeanDefinitions.add(keyspaceActionSpecificationBeanDefinition);

			} else if ("startup-cql".equals(name)) {
				startupScripts.add(parseScript(subElement));
			} else if ("shutdown-cql".equals(name)) {
				shutdownScripts.add(parseScript(subElement));
			}
		}

		builder.addPropertyValue("keyspaceSpecifications",
				getKeyspaceSetFlattenerBeanDefinition(element, context, keyspaceActionSpecificationBeanDefinitions));
		builder.addPropertyValue("startupScripts", startupScripts);
		builder.addPropertyValue("shutdownScripts", startupScripts);
	}

	/**
	 * Create the Single Factory Bean that will flatten all List<List<KeyspaceActionSpecificationFactoryBean>>
	 * 
	 * @param element
	 * @param context
	 * @param keyspaceActionSpecificationBeanDefinitions
	 * @return
	 */
	private Object getKeyspaceSetFlattenerBeanDefinition(Element element, ParserContext context,
			ManagedSet<BeanDefinition> keyspaceActionSpecificationBeanDefinitions) {

		BeanDefinitionBuilder flat = BeanDefinitionBuilder.genericBeanDefinition(MultiLevelSetFlattenerFactoryBean.class);
		flat.addPropertyValue("multiLevelSet", keyspaceActionSpecificationBeanDefinitions);
		return getSourceBeanDefinition(flat, context, element);

	}

	/**
	 * Parses the keyspace replication options and adds them to the supplied BeanDefinitionBuilder.
	 * 
	 * @param element
	 * @param builder
	 */
	/**
	 * @param element
	 * @param builder
	 */
	protected void parseReplication(Element element, BeanDefinitionBuilder builder) {

		if (element == null) {
			return;
		}

		String strategyClass = element.getAttribute("class");
		if (!StringUtils.hasText(strategyClass)) {
			strategyClass = KeyspaceAttributes.DEFAULT_REPLICATION_STRATEGY;
		}

		String replicationFactor = null;
		if (strategyClass.equals(ReplicationStrategy.SIMPLE_STRATEGY.getValue())) {

			replicationFactor = element.getAttribute("replication-factor");

			if (replicationFactor == null) {
				replicationFactor = KeyspaceAttributes.DEFAULT_REPLICATION_FACTOR + "";
			}

		}

		Map<Option, Object> replicationMap = new HashMap<Option, Object>();
		replicationMap.put(new DefaultOption("class", String.class, false, false, true), strategyClass);
		if (replicationFactor != null) {
			replicationMap.put(new DefaultOption("replication_factor", Long.class, true, false, false), replicationFactor);
		}

		/*
		 * DataCenters only apply to NetworkTolopogyStrategy
		 */
		if (strategyClass.equals(ReplicationStrategy.NETWORK_TOPOLOGY_STRATEGY.getValue())) {
			List<Element> dcElements = DomUtils.getChildElementsByTagName(element, "data-center");
			for (Element dataCenter : dcElements) {
				replicationMap.put(new DefaultOption(dataCenter.getAttribute("name"), Long.class, true, false, true),
						dataCenter.getAttribute("replication-factor"));
			}
		}

		builder.addPropertyValue("replicationOptions", replicationMap);

	}

	protected String parseScript(Element element) {
		return element.getTextContent();
	}

	protected BeanDefinition parsePoolingOptions(Element element) {
		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(PoolingOptionsConfig.class);

		ParsingUtils.setPropertyValue(builder, element, "min-simultaneous-requests", "minSimultaneousRequests");
		ParsingUtils.setPropertyValue(builder, element, "max-simultaneous-requests", "maxSimultaneousRequests");
		ParsingUtils.setPropertyValue(builder, element, "core-connections", "coreConnections");
		ParsingUtils.setPropertyValue(builder, element, "max-connections", "maxConnections");

		return builder.getBeanDefinition();
	}

	protected BeanDefinition parseSocketOptions(Element element) {
		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(SocketOptionsConfig.class);

		ParsingUtils.setPropertyValue(builder, element, "connect-timeout-mls", "connectTimeoutMls");
		ParsingUtils.setPropertyValue(builder, element, "keep-alive", "keepAlive");
		ParsingUtils.setPropertyValue(builder, element, "reuse-address", "reuseAddress");
		ParsingUtils.setPropertyValue(builder, element, "so-linger", "soLinger");
		ParsingUtils.setPropertyValue(builder, element, "tcp-no-delay", "tcpNoDelay");
		ParsingUtils.setPropertyValue(builder, element, "receive-buffer-size", "receiveBufferSize");
		ParsingUtils.setPropertyValue(builder, element, "send-buffer-size", "sendBufferSize");

		return builder.getBeanDefinition();
	}

	/**
	 * Returns a {@link BeanDefinition} for a {@link AuthProvider} object.
	 * 
	 * @param element
	 * @param context
	 * @return the {@link BeanDefinition} or {@literal null} if auth-info-provider is not given.
	 */
	private BeanDefinition getKeyspaceSpecificationBeanDefinition(Element element, ParserContext context) {

		String name = element.getAttribute("name");
		String action = element.getAttribute("action");
		String durableWrites = element.getAttribute("durable-writes");

		if (!StringUtils.hasText(action)) {
			return null;
		}

		BeanDefinitionBuilder keyspaceBuilder = BeanDefinitionBuilder
				.genericBeanDefinition(KeyspaceActionSpecificationFactoryBean.class);
		keyspaceBuilder.addPropertyValue("name", name);
		keyspaceBuilder.addPropertyValue("action", action);
		keyspaceBuilder.addPropertyValue("durableWrites", durableWrites);

		Element replicationElement = DomUtils.getChildElementByTagName(element, "replication");
		if (replicationElement != null) {
			parseReplication(replicationElement, keyspaceBuilder);
		}

		return getSourceBeanDefinition(keyspaceBuilder, context, element);
	}

}
