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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.springframework.cassandra.core.keyspace.KeyspaceOption.ReplicationStrategy;
import org.springframework.util.Assert;
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

	private final static Logger log = LoggerFactory.getLogger(CassandraClusterParser.class);

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

	/**
	 * Parses the attributes on the top level element, then parses all children.
	 * 
	 * @param element The Element being parsed
	 * @param context The Parser Context
	 * @param builder The parent {@link BeanDefinitionBuilder}
	 */
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

		String username = element.getAttribute("username");
		if (StringUtils.hasText(username)) {
			builder.addPropertyValue("username", username);
		}

		String password = element.getAttribute("password");
		if (StringUtils.hasText(password)) {
			builder.addPropertyValue("password", password);
		}

		String deferredInitialization = element.getAttribute("deferredInitialization");
		if (StringUtils.hasText(deferredInitialization)) {
			builder.addPropertyValue("deferredInitialization", deferredInitialization);
		}

		String metricsEnabled = element.getAttribute("metricsEnabled");
		if (StringUtils.hasText(metricsEnabled)) {
			builder.addPropertyValue("metricsEnabled", metricsEnabled);
		}

		String jmxReportingEnabled = element.getAttribute("jmxReportingEnabled");
		if (StringUtils.hasText(jmxReportingEnabled)) {
			builder.addPropertyValue("jmxReportingEnabled", jmxReportingEnabled);
		}

		String sslEnabled = element.getAttribute("sslEnabled");
		if (StringUtils.hasText(sslEnabled)) {
			builder.addPropertyValue("sslEnabled", sslEnabled);
		}

		String authProvider = element.getAttribute("auth-info-provider-ref");
		if (StringUtils.hasText(authProvider)) {
			builder.addPropertyReference("authProvider", authProvider);
		}

		String loadBalancingPolicy = element.getAttribute("load-balancing-policy-ref");
		if (StringUtils.hasText(loadBalancingPolicy)) {
			builder.addPropertyReference("loadBalancingPolicy", loadBalancingPolicy);
		}

		String reconnectionPolicy = element.getAttribute("reconnection-policy-ref");
		if (StringUtils.hasText(reconnectionPolicy)) {
			builder.addPropertyReference("reconnectionPolicy", reconnectionPolicy);
		}

		String retryPolicy = element.getAttribute("retry-policy-ref");
		if (StringUtils.hasText(retryPolicy)) {
			builder.addPropertyReference("retryPolicy", retryPolicy);
		}

		String sslOptions = element.getAttribute("ssl-options-ref");
		if (StringUtils.hasText(sslOptions)) {
			builder.addPropertyReference("sslOptions", sslOptions);
		}

		String hostStateListener = element.getAttribute("host-state-listener-ref");
		if (StringUtils.hasText(hostStateListener)) {
			builder.addPropertyReference("hostStateListener", hostStateListener);
		}

		String latencyTracker = element.getAttribute("latency-tracker-ref");
		if (StringUtils.hasText(latencyTracker)) {
			builder.addPropertyReference("latencyTracker", latencyTracker);
		}

		parseChildElements(element, context, builder);
	}

	/**
	 * Parse the Child Elemement of {@link BeanNames.CASSANDRA_CLUSTER}
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
		 * If the PoolingOptionsBuilder was initilized during parsing, process it now.
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
	 * Create the Single Factory Bean that will flatten all List<List<KeyspaceActionSpecificationFactoryBean>>
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
		String strategyClass = null;
		String replicationFactor = null;

		if (element != null) {

			strategyClass = element.getAttribute("class");
			if (!StringUtils.hasText(strategyClass)) {
				strategyClass = KeyspaceAttributes.DEFAULT_REPLICATION_STRATEGY;
			}

			replicationFactor = element.getAttribute("replication-factor");
			if (!StringUtils.hasText(replicationFactor)) {
				replicationFactor = KeyspaceAttributes.DEFAULT_REPLICATION_FACTOR + "";
			}

			/*
			 * DataCenters only apply to NetworkTolopogyStrategy
			 */
			List<Element> dcElements = DomUtils.getChildElementsByTagName(element, "data-center");
			for (Element dataCenter : dcElements) {
				networkTopologyDataCenters.add(dataCenter.getAttribute("name"));
				networkTopologyReplicationFactors.add(dataCenter.getAttribute("replication-factor"));
			}
		} else {
			strategyClass = ReplicationStrategy.SIMPLE_STRATEGY.name();
			replicationFactor = KeyspaceAttributes.DEFAULT_REPLICATION_FACTOR + "";
		}

		builder.addPropertyValue("replicationStrategy", strategyClass);
		builder.addPropertyValue("replicationFactor", replicationFactor);
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
			ParsingUtils.setPropertyValue(builder, element, "min-simultaneous-requests", "localMinSimultaneousRequests");
			ParsingUtils.setPropertyValue(builder, element, "max-simultaneous-requests", "localMaxSimultaneousRequests");
			ParsingUtils.setPropertyValue(builder, element, "core-connections", "localCoreConnections");
			ParsingUtils.setPropertyValue(builder, element, "max-connections", "localMaxConnections");
		}
		if (hostDistance.equals(HostDistance.REMOTE)) {
			ParsingUtils.setPropertyValue(builder, element, "min-simultaneous-requests", "remoteMinSimultaneousRequests");
			ParsingUtils.setPropertyValue(builder, element, "max-simultaneous-requests", "remoteMaxSimultaneousRequests");
			ParsingUtils.setPropertyValue(builder, element, "core-connections", "remoteCoreConnections");
			ParsingUtils.setPropertyValue(builder, element, "max-connections", "remoteMaxConnections");
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

		ParsingUtils.setPropertyValue(builder, element, "connect-timeout-mls", "connectTimeoutMillis");
		ParsingUtils.setPropertyValue(builder, element, "keep-alive", "keepAlive");
		ParsingUtils.setPropertyValue(builder, element, "read-timeout-mls", "readTimeoutMillis");
		ParsingUtils.setPropertyValue(builder, element, "reuse-address", "reuseAddress");
		ParsingUtils.setPropertyValue(builder, element, "so-linger", "soLinger");
		ParsingUtils.setPropertyValue(builder, element, "tcp-no-delay", "tcpNoDelay");
		ParsingUtils.setPropertyValue(builder, element, "receive-buffer-size", "receiveBufferSize");
		ParsingUtils.setPropertyValue(builder, element, "send-buffer-size", "sendBufferSize");

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

		String action = element.getAttribute("action");

		Assert.notNull(action, "Keyspace Action must not be null!");

		BeanDefinitionBuilder keyspaceBuilder = BeanDefinitionBuilder
				.genericBeanDefinition(KeyspaceActionSpecificationFactoryBean.class);

		ParsingUtils.setPropertyValue(keyspaceBuilder, element, "name", "name");
		ParsingUtils.setPropertyValue(keyspaceBuilder, element, "action", "action");
		ParsingUtils.setPropertyValue(keyspaceBuilder, element, "durableWrites", "durableWrites");

		Element replicationElement = DomUtils.getChildElementByTagName(element, "replication");
		parseReplication(replicationElement, keyspaceBuilder);

		return getSourceBeanDefinition(keyspaceBuilder, context, element);
	}

}
