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

import static org.springframework.cassandra.config.xml.ParsingUtils.addOptionalPropertyReference;
import static org.springframework.cassandra.config.xml.ParsingUtils.addOptionalPropertyValue;
import static org.springframework.cassandra.config.xml.ParsingUtils.addRequiredPropertyValue;
import static org.springframework.cassandra.config.xml.ParsingUtils.getSourceBeanDefinition;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.support.ManagedSet;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.cassandra.config.CassandraClusterFactoryBean;
import org.springframework.cassandra.config.KeyspaceActionSpecificationFactoryBean;
import org.springframework.cassandra.config.KeyspaceAttributes;
import org.springframework.cassandra.config.MultiLevelSetFlattenerFactoryBean;
import org.springframework.cassandra.config.PoolingOptionsFactoryBean;
import org.springframework.cassandra.config.SocketOptionsFactoryBean;
import org.springframework.cassandra.core.keyspace.KeyspaceActionSpecification;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.SocketOptions;

/**
 * Parses the {@literal <cluster>} element of the XML Configuration.
 * 
 * @author Matthew T. Adams
 * @author David Webb
 */
public class CassandraClusterParser extends AbstractBeanDefinitionParser {

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {

		String id = super.resolveId(element, definition, parserContext);
		return StringUtils.hasText(id) ? id : DefaultBeanNames.CLUSTER;
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

	/**
	 * Parses the attributes on the top level element, then parses all children.
	 * 
	 * @param element The Element being parsed
	 * @param context The Parser Context
	 * @param builder The parent {@link BeanDefinitionBuilder}
	 */
	protected void doParse(Element element, ParserContext context, BeanDefinitionBuilder builder) {

		addOptionalPropertyValue(builder, "contactPoints", element, "contact-points", null);
		addOptionalPropertyValue(builder, "port", element, "port", null);
		addOptionalPropertyValue(builder, "compressionType", element, "compression", null);
		addOptionalPropertyValue(builder, "username", element, "username", null);
		addOptionalPropertyValue(builder, "password", element, "password", null);
		addOptionalPropertyValue(builder, "deferredInitialization", element, "deferred-initialization", null);
		addOptionalPropertyValue(builder, "metricsEnabled", element, "metrics-enabled", null);
		addOptionalPropertyValue(builder, "jmxReportingEnabled", element, "jmx-reporting-enabled", null);
		addOptionalPropertyValue(builder, "sslEnabled", element, "ssl-enabled", null);

		addOptionalPropertyReference(builder, "authProvider", element, "auth-info-provider-ref", null);
		addOptionalPropertyReference(builder, "loadBalancingPolicy", element, "load-balancing-policy-ref", null);
		addOptionalPropertyReference(builder, "reconnectionPolicy", element, "reconnection-policy-ref", null);
		addOptionalPropertyReference(builder, "retryPolicy", element, "retry-policy-ref", null);
		addOptionalPropertyReference(builder, "sslOptions", element, "ssl-options-ref", null);
		addOptionalPropertyReference(builder, "hostStateListener", element, "host-state-listener-ref", null);
		addOptionalPropertyReference(builder, "latencyTracker", element, "latency-tracker-ref", null);

		parseChildElements(element, context, builder);
	}

	/**
	 * Parse the Child Element of {@link DefaultBeanNames.CLUSTER}
	 * 
	 * @param element The Element being parsed
	 * @param context The Parser Context
	 * @param builder The parent {@link BeanDefinitionBuilder}
	 */
	protected void parseChildElements(Element element, ParserContext context, BeanDefinitionBuilder builder) {

		ManagedSet<BeanDefinition> keyspaceActionSpecificationBeanDefinitions = new ManagedSet<BeanDefinition>();
		List<String> startupScripts = new ArrayList<String>();
		List<String> shutdownScripts = new ArrayList<String>();

		List<Element> elements = DomUtils.getChildElements(element);
		BeanDefinition keyspaceActionSpecificationBeanDefinition = null;

		/*
		 * PoolingOptionsBuilder has two potential parsing cycles so it is defined
		 * before the child elements are iterated over, then converted to a BeanDefinition
		 * just in time.
		 */
		BeanDefinitionBuilder poolingOptionsBuilder = null;

		/*
		 * Parse each of the child elements
		 */
		for (Element subElement : elements) {

			String name = subElement.getLocalName();

			if ("local-pooling-options".equals(name)) {
				poolingOptionsBuilder = parsePoolingOptions(subElement, poolingOptionsBuilder, HostDistance.LOCAL);
			} else if ("remote-pooling-options".equals(name)) {
				poolingOptionsBuilder = parsePoolingOptions(subElement, poolingOptionsBuilder, HostDistance.REMOTE);
			} else if ("socket-options".equals(name)) {
				builder.addPropertyValue("socketOptions", getSocketOptionsBeanDefinition(subElement, context));
			} else if ("keyspace".equals(name)) {

				keyspaceActionSpecificationBeanDefinition = getKeyspaceSpecificationBeanDefinition(subElement, context);
				keyspaceActionSpecificationBeanDefinitions.add(keyspaceActionSpecificationBeanDefinition);

			} else if ("startup-cql".equals(name)) {
				startupScripts.add(parseScript(subElement));
			} else if ("shutdown-cql".equals(name)) {
				shutdownScripts.add(parseScript(subElement));
			}
		}

		/*
		 * If the PoolingOptionsBuilder was initialized during parsing, process it now.
		 */
		if (poolingOptionsBuilder != null) {
			builder.addPropertyValue("poolingOptions", getSourceBeanDefinition(poolingOptionsBuilder, context, element));
		}

		builder.addPropertyValue("keyspaceSpecifications",
				getKeyspaceSetFlattenerBeanDefinition(element, context, keyspaceActionSpecificationBeanDefinitions));
		builder.addPropertyValue("startupScripts", startupScripts);
		builder.addPropertyValue("shutdownScripts", startupScripts);
	}

	/**
	 * Create the Single Factory Bean that will flatten all Set<Set<KeyspaceActionSpecificationFactoryBean>>
	 * 
	 * @param element The Element being parsed
	 * @param context The Parser Context
	 * @param keyspaceActionSpecificationBeanDefinitions The List of Definitions to flatten
	 * @return A single level List of KeyspaceActionSpecifications
	 */
	private Object getKeyspaceSetFlattenerBeanDefinition(Element element, ParserContext context,
			ManagedSet<BeanDefinition> keyspaceActionSpecificationBeanDefinitions) {

		BeanDefinitionBuilder flat = BeanDefinitionBuilder.genericBeanDefinition(MultiLevelSetFlattenerFactoryBean.class);
		flat.addPropertyValue("multiLevelSet", keyspaceActionSpecificationBeanDefinitions);
		return getSourceBeanDefinition(flat, context, element);

	}

	/**
	 * Parses the keyspace replication options and adds them to the supplied {@link BeanDefinitionBuilder}.
	 * 
	 * @param element The Element being parsed
	 * @param builder The {@link BeanDefinitionBuilder} to add the replication to
	 */
	protected void parseReplication(Element element, BeanDefinitionBuilder builder) {

		ManagedList<String> networkTopologyDataCenters = new ManagedList<String>();
		ManagedList<String> networkTopologyReplicationFactors = new ManagedList<String>();

		if (element != null) {

			addOptionalPropertyValue(builder, "replicationStrategy", element, "class",
					KeyspaceAttributes.DEFAULT_REPLICATION_STRATEGY.name());
			addOptionalPropertyValue(builder, "replicationFactor", element, "replication-factor", ""
					+ KeyspaceAttributes.DEFAULT_REPLICATION_FACTOR);

			/*
			 * DataCenters only apply to NetworkTolopogyStrategy
			 */
			List<Element> dcElements = DomUtils.getChildElementsByTagName(element, "data-center");
			for (Element dataCenter : dcElements) {
				networkTopologyDataCenters.add(dataCenter.getAttribute("name"));
				networkTopologyReplicationFactors.add(dataCenter.getAttribute("replication-factor"));
			}
		}

		builder.addPropertyValue("networkTopologyDataCenters", networkTopologyDataCenters);
		builder.addPropertyValue("networkTopologyReplicationFactors", networkTopologyReplicationFactors);
	}

	/**
	 * Parse CQL Script Elements
	 * 
	 * @param element The Element being parsed
	 * @return
	 */
	protected String parseScript(Element element) {
		return element.getTextContent();
	}

	/**
	 * Returns a {@link BeanDefinition} for a {@link PoolingOptions} object.
	 * 
	 * @param element The Element being parsed
	 * @param builder The {@link BeanDefinition} to use for building if one already exists
	 * @param hostDistance The scope of the PoolingOptions to apply
	 * @return The {@link BeanDefinitionBuilder}
	 */
	protected BeanDefinitionBuilder parsePoolingOptions(Element element, BeanDefinitionBuilder builder,
			HostDistance hostDistance) {

		if (builder == null) {
			builder = BeanDefinitionBuilder.genericBeanDefinition(PoolingOptionsFactoryBean.class);
		}

		if (hostDistance.equals(HostDistance.LOCAL)) {
			addOptionalPropertyValue(builder, "localMinSimultaneousRequests", element, "min-simultaneous-requests", null);
			addOptionalPropertyValue(builder, "localMaxSimultaneousRequests", element, "max-simultaneous-requests", null);
			addOptionalPropertyValue(builder, "localCoreConnections", element, "core-connections", null);
			addOptionalPropertyValue(builder, "localMaxConnections", element, "max-connections", null);
		}
		if (hostDistance.equals(HostDistance.REMOTE)) {
			addOptionalPropertyValue(builder, "remoteMinSimultaneousRequests", element, "min-simultaneous-requests", null);
			addOptionalPropertyValue(builder, "remoteMaxSimultaneousRequests", element, "max-simultaneous-requests", null);
			addOptionalPropertyValue(builder, "remoteCoreConnections", element, "core-connections", null);
			addOptionalPropertyValue(builder, "remoteMaxConnections", element, "max-connections", null);
		}

		return builder;
	}

	/**
	 * Returns a {@link BeanDefinition} for a {@link SocketOptions} object.
	 * 
	 * @param element The Element being parsed
	 * @param context The ParserContext
	 * @return The {@link BeanDefinition}
	 */
	protected BeanDefinition getSocketOptionsBeanDefinition(Element element, ParserContext context) {

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(SocketOptionsFactoryBean.class);

		addOptionalPropertyValue(builder, "connectTimeoutMillis", element, "connect-timeout-mls", null);
		addOptionalPropertyValue(builder, "keepAlive", element, "keep-alive", null);
		addOptionalPropertyValue(builder, "readTimeoutMillis", element, "read-timeout-mls", null);
		addOptionalPropertyValue(builder, "reuseAddress", element, "reuse-address", null);
		addOptionalPropertyValue(builder, "soLinger", element, "so-linger", null);
		addOptionalPropertyValue(builder, "tcpNoDelay", element, "tcp-no-delay", null);
		addOptionalPropertyValue(builder, "receiveBufferSize", element, "receive-buffer-size", null);
		addOptionalPropertyValue(builder, "sendBufferSize", element, "send-buffer-size", null);

		return getSourceBeanDefinition(builder, context, element);
	}

	/**
	 * Returns a {@link BeanDefinition} for a {@link KeyspaceActionSpecification} object.
	 * 
	 * @param element The Element being parsed
	 * @param context The Parser Context
	 * @return The {@link BeanDefinition} or {@literal null} if action is not given.
	 */
	private BeanDefinition getKeyspaceSpecificationBeanDefinition(Element element, ParserContext context) {

		BeanDefinitionBuilder keyspaceBuilder = BeanDefinitionBuilder
				.genericBeanDefinition(KeyspaceActionSpecificationFactoryBean.class);

		// add required replication defaults
		addRequiredPropertyValue(keyspaceBuilder, "replicationStrategy",
				KeyspaceAttributes.DEFAULT_REPLICATION_STRATEGY.name());
		addRequiredPropertyValue(keyspaceBuilder, "replicationFactor", "" + KeyspaceAttributes.DEFAULT_REPLICATION_FACTOR);

		// now start parsing
		addRequiredPropertyValue(keyspaceBuilder, "name", element, "name");
		addRequiredPropertyValue(keyspaceBuilder, "action", element, "action");
		addOptionalPropertyValue(keyspaceBuilder, "durableWrites", element, "durable-writes", "false");

		Element replicationElement = DomUtils.getChildElementByTagName(element, "replication");
		parseReplication(replicationElement, keyspaceBuilder);

		return getSourceBeanDefinition(keyspaceBuilder, context, element);
	}
}
