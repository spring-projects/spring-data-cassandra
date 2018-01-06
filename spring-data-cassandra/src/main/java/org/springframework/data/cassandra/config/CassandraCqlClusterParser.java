/*
 * Copyright 2013-2018 the original author or authors.
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

import static org.springframework.data.cassandra.config.ParsingUtils.*;

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
import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceActionSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceAttributes;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.SocketOptions;

/**
 * Parses the {@literal <cluster>} element of the XML Configuration.
 *
 * @author Matthew T. Adams
 * @author David Webb
 * @author John Blum
 * @author Mark Paluch
 */
class CassandraCqlClusterParser extends AbstractBeanDefinitionParser {

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.xml.AbstractBeanDefinitionParser#resolveId(org.w3c.dom.Element, org.springframework.beans.factory.support.AbstractBeanDefinition, org.springframework.beans.factory.xml.ParserContext)
	 */
	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {

		String id = super.resolveId(element, definition, parserContext);

		return StringUtils.hasText(id) ? id : DefaultCqlBeanNames.CLUSTER;
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.xml.AbstractBeanDefinitionParser#parseInternal(org.w3c.dom.Element, org.springframework.beans.factory.xml.ParserContext)
	 */
	@Override
	protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(CassandraClusterFactoryBean.class);

		builder.setLazyInit(parserContext.isDefaultLazyInit());
		builder.getRawBeanDefinition().setDestroyMethodName("destroy");
		builder.getRawBeanDefinition().setSource(parserContext.extractSource(element));

		if (parserContext.isNested()) {
			// inner bean definitions must have same scope as containing bean
			builder.setScope(parserContext.getContainingBeanDefinition().getScope());
		}

		doParse(element, parserContext, builder);

		return builder.getBeanDefinition();
	}

	/**
	 * Parses cluster meta-data.
	 *
	 * @param element {@link Element} to parse.
	 * @param parserContext XML parser context and state.
	 * @param builder parent {@link BeanDefinitionBuilder}.
	 */
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		addOptionalPropertyReference(builder, "addressTranslator", element, "address-translator-ref");
		addOptionalPropertyReference(builder, "authProvider", element, "auth-info-provider-ref");
		addOptionalPropertyReference(builder, "clusterBuilderConfigurer", element, "cluster-builder-configurer-ref");
		addOptionalPropertyReference(builder, "hostStateListener", element, "host-state-listener-ref");
		addOptionalPropertyReference(builder, "latencyTracker", element, "latency-tracker-ref");
		addOptionalPropertyReference(builder, "loadBalancingPolicy", element, "load-balancing-policy-ref");
		addOptionalPropertyReference(builder, "nettyOptions", element, "netty-options-ref");
		addOptionalPropertyReference(builder, "reconnectionPolicy", element, "reconnection-policy-ref");
		addOptionalPropertyReference(builder, "retryPolicy", element, "retry-policy-ref");
		addOptionalPropertyReference(builder, "speculativeExecutionPolicy", element, "speculative-execution-policy-ref");
		addOptionalPropertyReference(builder, "sslOptions", element, "ssl-options-ref");
		addOptionalPropertyReference(builder, "timestampGenerator", element, "timestamp-generator-ref");

		addOptionalPropertyValue(builder, "clusterName", element, "cluster-name");
		addOptionalPropertyValue(builder, "contactPoints", element, "contact-points");
		addOptionalPropertyValue(builder, "compressionType", element, "compression");
		addOptionalPropertyValue(builder, "jmxReportingEnabled", element, "jmx-reporting-enabled");
		addOptionalPropertyValue(builder, "maxSchemaAgreementWaitSeconds", element, "max-schema-agreement-wait-seconds");
		addOptionalPropertyValue(builder, "metricsEnabled", element, "metrics-enabled");
		addOptionalPropertyValue(builder, "password", element, "password");
		addOptionalPropertyValue(builder, "port", element, "port");
		addOptionalPropertyValue(builder, "sslEnabled", element, "ssl-enabled");
		addOptionalPropertyValue(builder, "username", element, "username");

		parseChildElements(element, parserContext, builder);
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

		BeanDefinitionBuilder poolingOptionsBuilder = BeanDefinitionBuilder
				.genericBeanDefinition(PoolingOptionsFactoryBean.class);

		addOptionalPropertyReference(poolingOptionsBuilder, "initializationExecutor", element,
				"initialization-executor-ref");

		addOptionalPropertyValue(poolingOptionsBuilder, "heartbeatIntervalSeconds", element, "heartbeat-interval-seconds");
		addOptionalPropertyValue(poolingOptionsBuilder, "idleTimeoutSeconds", element, "idle-timeout-seconds");
		addOptionalPropertyValue(poolingOptionsBuilder, "poolTimeoutMilliseconds", element, "pool-timeout-milliseconds");
		addOptionalPropertyValue(poolingOptionsBuilder, "maxQueueSize", element, "max-queue-size");

		// parse child elements
		for (Element subElement : DomUtils.getChildElements(element)) {

			String name = subElement.getLocalName();

			if ("keyspace".equals(name)) {
				keyspaceActionSpecificationBeanDefinitions
						.add(newKeyspaceActionSpecificationBeanDefinition(subElement, parserContext));
			} else if ("local-pooling-options".equals(name)) {
				parseLocalPoolingOptions(subElement, poolingOptionsBuilder);
			} else if ("remote-pooling-options".equals(name)) {
				parseRemotePoolingOptions(subElement, poolingOptionsBuilder);
			} else if ("socket-options".equals(name)) {
				builder.addPropertyValue("socketOptions", newSocketOptionsBeanDefinition(subElement, parserContext));
			} else if ("startup-cql".equals(name)) {
				startupScripts.add(parseScript(subElement));
			} else if ("shutdown-cql".equals(name)) {
				shutdownScripts.add(parseScript(subElement));
			}
		}

		builder.addPropertyValue("keyspaceActions", keyspaceActionSpecificationBeanDefinitions);
		builder.addPropertyValue("poolingOptions", getSourceBeanDefinition(poolingOptionsBuilder, parserContext, element));
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

		parseReplication(DomUtils.getChildElementByTagName(element, "replication"), builder);

		return getSourceBeanDefinition(builder, parserContext, element);
	}

	/**
	 * Parses the keyspace replication options and adds them to the supplied {@link BeanDefinitionBuilder}.
	 *
	 * @param element {@link Element} to parse.
	 * @param builder The {@link BeanDefinitionBuilder} to add the replication to
	 */
	private void parseReplication(@Nullable Element element, BeanDefinitionBuilder builder) {

		ManagedList<String> networkTopologyDataCenters = new ManagedList<>();
		ManagedList<String> networkTopologyReplicationFactors = new ManagedList<>();

		if (element != null) {
			addOptionalPropertyValue(builder, "replicationStrategy", element, "class",
					KeyspaceAttributes.DEFAULT_REPLICATION_STRATEGY.name());

			addOptionalPropertyValue(builder, "replicationFactor", element, "replication-factor",
					String.valueOf(KeyspaceAttributes.DEFAULT_REPLICATION_FACTOR));

			// DataCenters only apply to NetworkTopologyStrategy
			for (Element dataCenter : DomUtils.getChildElementsByTagName(element, "data-center")) {
				networkTopologyDataCenters.add(dataCenter.getAttribute("name"));
				networkTopologyReplicationFactors.add(dataCenter.getAttribute("replication-factor"));
			}
		}

		builder.addPropertyValue("networkTopologyDataCenters", networkTopologyDataCenters);
		builder.addPropertyValue("networkTopologyReplicationFactors", networkTopologyReplicationFactors);
	}

	/**
	 * Parses local pooling options.
	 *
	 * @param element {@link Element} to parse.
	 * @param builder {@link BeanDefinitionBuilder} used to build a {@link PoolingOptions} {@link BeanDefinition}.
	 */
	void parseLocalPoolingOptions(Element element, BeanDefinitionBuilder builder) {

		addOptionalPropertyValue(builder, "localCoreConnections", element, "core-connections", null);
		addOptionalPropertyValue(builder, "localMaxConnections", element, "max-connections", null);
		addOptionalPropertyValue(builder, "localMaxSimultaneousRequests", element, "max-simultaneous-requests", null);
		addOptionalPropertyValue(builder, "localMinSimultaneousRequests", element, "min-simultaneous-requests", null);
	}

	/**
	 * Parses remote pooling options.
	 *
	 * @param element {@link Element} to parse.
	 * @param builder {@link BeanDefinitionBuilder} used to build a {@link PoolingOptions} {@link BeanDefinition}.
	 */
	void parseRemotePoolingOptions(Element element, BeanDefinitionBuilder builder) {

		addOptionalPropertyValue(builder, "remoteCoreConnections", element, "core-connections", null);
		addOptionalPropertyValue(builder, "remoteMaxConnections", element, "max-connections", null);
		addOptionalPropertyValue(builder, "remoteMaxSimultaneousRequests", element, "max-simultaneous-requests", null);
		addOptionalPropertyValue(builder, "remoteMinSimultaneousRequests", element, "min-simultaneous-requests", null);
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
	 * Returns a {@link BeanDefinition} for a {@link SocketOptions} object.
	 *
	 * @param element {@link Element} to parse.
	 * @param parserContext XML parser context and state.
	 * @return {@link BeanDefinition} for {@link SocketOptionsFactoryBean}.
	 */
	BeanDefinition newSocketOptionsBeanDefinition(Element element, ParserContext parserContext) {

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(SocketOptionsFactoryBean.class);

		addOptionalPropertyValue(builder, "connectTimeoutMillis", element, "connect-timeout-millis");
		addOptionalPropertyValue(builder, "keepAlive", element, "keep-alive");
		addOptionalPropertyValue(builder, "readTimeoutMillis", element, "read-timeout-millis");
		addOptionalPropertyValue(builder, "receiveBufferSize", element, "receive-buffer-size");
		addOptionalPropertyValue(builder, "reuseAddress", element, "reuse-address");
		addOptionalPropertyValue(builder, "sendBufferSize", element, "send-buffer-size");
		addOptionalPropertyValue(builder, "soLinger", element, "so-linger");
		addOptionalPropertyValue(builder, "tcpNoDelay", element, "tcp-no-delay");

		return getSourceBeanDefinition(builder, parserContext, element);
	}
}
