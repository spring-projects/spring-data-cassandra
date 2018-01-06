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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.springframework.data.cassandra.support.BeanDefinitionTestUtils.*;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.parsing.PassThroughSourceExtractor;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.BeanDefinitionParserDelegate;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.beans.factory.xml.XmlReaderContext;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Test suite of Unit tests testing the contract and functionality of the {@link CassandraCqlClusterParser}.
 *
 * @author John Blum
 * @author Mark Paluch
 */
// TODO add more tests!
@RunWith(MockitoJUnitRunner.class)
public class CassandraCqlClusterParserUnitTests {

	@Mock Element mockElement;

	private CassandraCqlClusterParser parser = new CassandraCqlClusterParser();

	@Test // DATACASS-298
	public void resolveIdFromElement() {

		when(mockElement.getAttribute(eq(CassandraCqlClusterParser.ID_ATTRIBUTE))).thenReturn("test");

		assertThat(parser.resolveId(mockElement, null, null)).isEqualTo("test");
		verify(mockElement).getAttribute(eq(CassandraCqlClusterParser.ID_ATTRIBUTE));
	}

	@Test // DATACASS-298
	public void resolveIdUsingDefault() {

		when(mockElement.getAttribute(eq(CassandraCqlClusterParser.ID_ATTRIBUTE))).thenReturn("");

		assertThat(parser.resolveId(mockElement, null, null)).isEqualTo(DefaultCqlBeanNames.CLUSTER);
		verify(mockElement).getAttribute(eq(CassandraCqlClusterParser.ID_ATTRIBUTE));
	}

	@Test // DATACASS-298
	public void parseInternalCallsDoParseAndConstructsBeanDefinition() {

		BeanDefinition mockContainingBeanDefinition = mock(BeanDefinition.class);

		when(mockContainingBeanDefinition.getScope()).thenReturn("Singleton");
		when(mockElement.getAttribute("address-translator-ref")).thenReturn("testAddressTranslator");
		when(mockElement.getAttribute("auth-info-provider-ref")).thenReturn("testAuthInfoProvider");
		when(mockElement.getAttribute("cluster-builder-configurer-ref")).thenReturn("testClusterBuilderConfigurer");
		when(mockElement.getAttribute("host-state-listener-ref")).thenReturn("testHostStateListener");
		when(mockElement.getAttribute("latency-tracker-ref")).thenReturn("testLatencyTracker");
		when(mockElement.getAttribute("load-balancing-policy-ref")).thenReturn("testLoadBalancingPolicy");
		when(mockElement.getAttribute("netty-options-ref")).thenReturn("testNettyOptions");
		when(mockElement.getAttribute("reconnection-policy-ref")).thenReturn("testReconnectionPolicy");
		when(mockElement.getAttribute("retry-policy-ref")).thenReturn("testRetryPolicy");
		when(mockElement.getAttribute("speculative-execution-policy-ref")).thenReturn("testSpeculativeExecutionPolicy");
		when(mockElement.getAttribute("ssl-options-ref")).thenReturn("testSslOptions");
		when(mockElement.getAttribute("timestamp-generator-ref")).thenReturn("testTimestampGenerator");
		when(mockElement.getAttribute("cluster-name")).thenReturn("testCluster");
		when(mockElement.getAttribute("contact-points")).thenReturn("skullbox");
		when(mockElement.getAttribute("compression")).thenReturn("SNAPPY");
		when(mockElement.getAttribute("jmx-reporting-enabled")).thenReturn("true");
		when(mockElement.getAttribute("max-schema-agreement-wait-seconds")).thenReturn("30");
		when(mockElement.getAttribute("metrics-enabled")).thenReturn("true");
		when(mockElement.getAttribute("password")).thenReturn("p@55w0rd");
		when(mockElement.getAttribute("port")).thenReturn("12345");
		when(mockElement.getAttribute("ssl-enabled")).thenReturn("true");
		when(mockElement.getAttribute("username")).thenReturn("jonDoe");

		CassandraCqlClusterParser parser = new CassandraCqlClusterParser() {
			@Override
			protected void parseChildElements(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {}
		};

		AbstractBeanDefinition beanDefinition = parser.parseInternal(mockElement,
				mockParserContext(mockContainingBeanDefinition));

		assertThat(beanDefinition).isNotNull();
		assertThat(beanDefinition.getBeanClassName()).isEqualTo(CassandraClusterFactoryBean.class.getName());
		assertThat(beanDefinition.getDestroyMethodName()).isEqualTo("destroy");
		assertThat((Element) beanDefinition.getSource()).isEqualTo(mockElement);
		assertThat(beanDefinition.isLazyInit()).isFalse();
		assertThat(getPropertyValueAsString(beanDefinition, "addressTranslator")).isEqualTo("testAddressTranslator");
		assertThat(getPropertyValueAsString(beanDefinition, "authProvider")).isEqualTo("testAuthInfoProvider");
		assertThat(getPropertyValueAsString(beanDefinition, "clusterBuilderConfigurer"))
				.isEqualTo("testClusterBuilderConfigurer");
		assertThat(getPropertyValueAsString(beanDefinition, "hostStateListener")).isEqualTo("testHostStateListener");
		assertThat(getPropertyValueAsString(beanDefinition, "latencyTracker")).isEqualTo("testLatencyTracker");
		assertThat(getPropertyValueAsString(beanDefinition, "loadBalancingPolicy")).isEqualTo("testLoadBalancingPolicy");
		assertThat(getPropertyValueAsString(beanDefinition, "nettyOptions")).isEqualTo("testNettyOptions");
		assertThat(getPropertyValueAsString(beanDefinition, "reconnectionPolicy")).isEqualTo("testReconnectionPolicy");
		assertThat(getPropertyValueAsString(beanDefinition, "retryPolicy")).isEqualTo("testRetryPolicy");
		assertThat(getPropertyValueAsString(beanDefinition, "speculativeExecutionPolicy"))
				.isEqualTo("testSpeculativeExecutionPolicy");
		assertThat(getPropertyValueAsString(beanDefinition, "sslOptions")).isEqualTo("testSslOptions");
		assertThat(getPropertyValueAsString(beanDefinition, "timestampGenerator")).isEqualTo("testTimestampGenerator");
		assertThat(getPropertyValueAsString(beanDefinition, "clusterName")).isEqualTo("testCluster");
		assertThat(getPropertyValueAsString(beanDefinition, "contactPoints")).isEqualTo("skullbox");
		assertThat(getPropertyValueAsString(beanDefinition, "compressionType")).isEqualTo("SNAPPY");
		assertThat(getPropertyValueAsString(beanDefinition, "jmxReportingEnabled")).isEqualTo("true");
		assertThat(getPropertyValueAsString(beanDefinition, "maxSchemaAgreementWaitSeconds")).isEqualTo("30");
		assertThat(getPropertyValueAsString(beanDefinition, "metricsEnabled")).isEqualTo("true");
		assertThat(getPropertyValueAsString(beanDefinition, "password")).isEqualTo("p@55w0rd");
		assertThat(getPropertyValueAsString(beanDefinition, "port")).isEqualTo("12345");
		assertThat(getPropertyValueAsString(beanDefinition, "sslEnabled")).isEqualTo("true");
		assertThat(getPropertyValueAsString(beanDefinition, "username")).isEqualTo("jonDoe");

		verify(mockContainingBeanDefinition).getScope();
		verify(mockElement).getAttribute(eq("address-translator-ref"));
		verify(mockElement).getAttribute(eq("auth-info-provider-ref"));
		verify(mockElement).getAttribute(eq("cluster-builder-configurer-ref"));
		verify(mockElement).getAttribute(eq("host-state-listener-ref"));
		verify(mockElement).getAttribute(eq("latency-tracker-ref"));
		verify(mockElement).getAttribute(eq("load-balancing-policy-ref"));
		verify(mockElement).getAttribute(eq("netty-options-ref"));
		verify(mockElement).getAttribute(eq("reconnection-policy-ref"));
		verify(mockElement).getAttribute(eq("retry-policy-ref"));
		verify(mockElement).getAttribute(eq("speculative-execution-policy-ref"));
		verify(mockElement).getAttribute(eq("timestamp-generator-ref"));
		verify(mockElement).getAttribute(eq("ssl-options-ref"));
		verify(mockElement).getAttribute(eq("cluster-name"));
		verify(mockElement).getAttribute(eq("contact-points"));
		verify(mockElement).getAttribute(eq("compression"));
		verify(mockElement).getAttribute(eq("jmx-reporting-enabled"));
		verify(mockElement).getAttribute(eq("max-schema-agreement-wait-seconds"));
		verify(mockElement).getAttribute(eq("metrics-enabled"));
		verify(mockElement).getAttribute(eq("password"));
		verify(mockElement).getAttribute(eq("port"));
		verify(mockElement).getAttribute(eq("ssl-enabled"));
		verify(mockElement).getAttribute(eq("username"));
	}

	@Test // DATACASS-298
	public void parseChildElementsWithLocalPoolingOptions() {

		Element localPoolingOptionsElement = mock(Element.class);

		NodeList mockNodeList = mockNodeList(localPoolingOptionsElement);

		when(localPoolingOptionsElement.getLocalName()).thenReturn("local-pooling-options");
		when(localPoolingOptionsElement.getAttribute(eq("core-connections"))).thenReturn("50");
		when(localPoolingOptionsElement.getAttribute(eq("max-connections"))).thenReturn("200");
		when(localPoolingOptionsElement.getAttribute(eq("max-simultaneous-requests"))).thenReturn("50");
		when(localPoolingOptionsElement.getAttribute(eq("min-simultaneous-requests"))).thenReturn("5");
		when(mockElement.getChildNodes()).thenReturn(mockNodeList);
		when(mockElement.getAttribute(eq("heartbeat-interval-seconds"))).thenReturn("15");
		when(mockElement.getAttribute(eq("idle-timeout-seconds"))).thenReturn("120");
		when(mockElement.getAttribute(eq("initialization-executor-ref"))).thenReturn("testExecutor");
		when(mockElement.getAttribute(eq("pool-timeout-milliseconds"))).thenReturn("60000");

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition();

		parser.parseChildElements(mockElement, mockParserContext(null), builder);

		BeanDefinition beanDefinition = builder.getBeanDefinition();

		BeanDefinition poolingOptionsBeanDefinition = getPropertyValue(beanDefinition, "poolingOptions");

		assertThat(poolingOptionsBeanDefinition).isNotNull();
		assertThat(poolingOptionsBeanDefinition.getBeanClassName()).isEqualTo(PoolingOptionsFactoryBean.class.getName());
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "heartbeatIntervalSeconds")).isEqualTo("15");
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "idleTimeoutSeconds")).isEqualTo("120");
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "initializationExecutor"))
				.isEqualTo("testExecutor");
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "poolTimeoutMilliseconds")).isEqualTo("60000");
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "localCoreConnections")).isEqualTo("50");
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "localMaxConnections")).isEqualTo("200");
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "localMaxSimultaneousRequests")).isEqualTo("50");
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "localMinSimultaneousRequests")).isEqualTo("5");
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "remoteCoreConnections")).isNull();
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "remoteMaxConnections")).isNull();
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "remoteMaxSimultaneousRequests")).isNull();
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "remoteMinSimultaneousRequests")).isNull();

		verify(mockElement).getChildNodes();
		verify(mockElement).getAttribute(eq("heartbeat-interval-seconds"));
		verify(mockElement).getAttribute(eq("idle-timeout-seconds"));
		verify(mockElement).getAttribute(eq("initialization-executor-ref"));
		verify(mockElement).getAttribute(eq("pool-timeout-milliseconds"));
		verify(localPoolingOptionsElement).getLocalName();
		verify(localPoolingOptionsElement).getAttribute(eq("core-connections"));
		verify(localPoolingOptionsElement).getAttribute(eq("max-connections"));
		verify(localPoolingOptionsElement).getAttribute(eq("max-simultaneous-requests"));
		verify(localPoolingOptionsElement).getAttribute(eq("min-simultaneous-requests"));
	}

	@Test // DATACASS-298
	public void parseChildElementsWithRemotePoolingOptions() {

		Element localPoolingOptionsElement = mock(Element.class);

		NodeList mockNodeList = mockNodeList(localPoolingOptionsElement);

		when(localPoolingOptionsElement.getLocalName()).thenReturn("remote-pooling-options");
		when(localPoolingOptionsElement.getAttribute(eq("core-connections"))).thenReturn("50");
		when(localPoolingOptionsElement.getAttribute(eq("max-connections"))).thenReturn("200");
		when(localPoolingOptionsElement.getAttribute(eq("max-simultaneous-requests"))).thenReturn("50");
		when(localPoolingOptionsElement.getAttribute(eq("min-simultaneous-requests"))).thenReturn("5");
		when(mockElement.getChildNodes()).thenReturn(mockNodeList);
		when(mockElement.getAttribute(eq("heartbeat-interval-seconds"))).thenReturn("15");
		when(mockElement.getAttribute(eq("idle-timeout-seconds"))).thenReturn("120");
		when(mockElement.getAttribute(eq("initialization-executor-ref"))).thenReturn("testExecutor");
		when(mockElement.getAttribute(eq("pool-timeout-milliseconds"))).thenReturn("60000");

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition();

		parser.parseChildElements(mockElement, mockParserContext(null), builder);

		BeanDefinition beanDefinition = builder.getBeanDefinition();

		BeanDefinition poolingOptionsBeanDefinition = getPropertyValue(beanDefinition, "poolingOptions");

		assertThat(poolingOptionsBeanDefinition).isNotNull();
		assertThat(poolingOptionsBeanDefinition.getBeanClassName()).isEqualTo(PoolingOptionsFactoryBean.class.getName());
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "heartbeatIntervalSeconds")).isEqualTo("15");
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "idleTimeoutSeconds")).isEqualTo("120");
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "initializationExecutor"))
				.isEqualTo("testExecutor");
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "poolTimeoutMilliseconds")).isEqualTo("60000");
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "localCoreConnections")).isNull();
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "localMaxConnections")).isNull();
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "localMaxSimultaneousRequests")).isNull();
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "localMinSimultaneousRequests")).isNull();
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "remoteCoreConnections")).isEqualTo("50");
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "remoteMaxConnections")).isEqualTo("200");
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "remoteMaxSimultaneousRequests")).isEqualTo("50");
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "remoteMinSimultaneousRequests")).isEqualTo("5");

		verify(mockElement).getChildNodes();
		verify(mockElement).getAttribute(eq("heartbeat-interval-seconds"));
		verify(mockElement).getAttribute(eq("idle-timeout-seconds"));
		verify(mockElement).getAttribute(eq("initialization-executor-ref"));
		verify(mockElement).getAttribute(eq("pool-timeout-milliseconds"));
		verify(localPoolingOptionsElement).getLocalName();
		verify(localPoolingOptionsElement).getAttribute(eq("core-connections"));
		verify(localPoolingOptionsElement).getAttribute(eq("max-connections"));
		verify(localPoolingOptionsElement).getAttribute(eq("max-simultaneous-requests"));
		verify(localPoolingOptionsElement).getAttribute(eq("min-simultaneous-requests"));
	}

	@Test // DATACASS-242
	public void parseChildElementsWithStartupAndShutdownScripts() {

		Element mockStartupCqlOne = mock(Element.class, "MockStartupCqlOne");
		Element mockStartupCqlTwo = mock(Element.class, "MockStartupCqlTwo");
		Element mockShutdownCqlOne = mock(Element.class, "MockShutdownCqlOne");
		Element mockShutdownCqlTwo = mock(Element.class, "MockShutdownCqlTwo");

		when(mockStartupCqlOne.getLocalName()).thenReturn("startup-cql");
		when(mockStartupCqlTwo.getLocalName()).thenReturn("startup-cql");
		when(mockStartupCqlOne.getTextContent()).thenReturn("CREATE KEYSPACE test;");
		when(mockStartupCqlTwo.getTextContent()).thenReturn("CREATE TABLE test.table;");
		when(mockShutdownCqlOne.getLocalName()).thenReturn("shutdown-cql");
		when(mockShutdownCqlTwo.getLocalName()).thenReturn("shutdown-cql");
		when(mockShutdownCqlOne.getTextContent()).thenReturn("DROP KEYSPACE test;");
		when(mockShutdownCqlTwo.getTextContent()).thenReturn("DROP USER jblum;");

		NodeList mockNodeList = mockNodeList(mockStartupCqlOne, mockStartupCqlTwo, mockShutdownCqlOne, mockShutdownCqlTwo);

		when(mockElement.getChildNodes()).thenReturn(mockNodeList);

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition();

		parser.parseChildElements(mockElement, mockParserContext(null), builder);

		BeanDefinition beanDefinition = builder.getBeanDefinition();

		List<String> startupScripts = getPropertyValue(beanDefinition, "startupScripts");
		assertThat(startupScripts).contains("CREATE KEYSPACE test;", "CREATE TABLE test.table;");

		List<String> shutdownScripts = getPropertyValue(beanDefinition, "shutdownScripts");
		assertThat(shutdownScripts).contains("DROP KEYSPACE test;", "DROP USER jblum;");
	}

	@Test // DATACASS-298
	public void parseLocalPoolingOptionsProperlyConfiguresBeanDefinition() {

		when(mockElement.getAttribute(eq("core-connections"))).thenReturn("50");
		when(mockElement.getAttribute(eq("max-connections"))).thenReturn("200");
		when(mockElement.getAttribute(eq("max-simultaneous-requests"))).thenReturn("50");
		when(mockElement.getAttribute(eq("min-simultaneous-requests"))).thenReturn("5");

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition();

		parser.parseLocalPoolingOptions(mockElement, builder);

		BeanDefinition beanDefinition = builder.getBeanDefinition();

		assertThat(getPropertyValueAsString(beanDefinition, "heartbeatIntervalSeconds")).isNull();
		assertThat(getPropertyValueAsString(beanDefinition, "idleTimeoutSeconds")).isNull();
		assertThat(getPropertyValueAsString(beanDefinition, "initializationExecutor")).isNull();
		assertThat(getPropertyValueAsString(beanDefinition, "poolTimeoutMilliseconds")).isNull();
		assertThat(getPropertyValueAsString(beanDefinition, "localCoreConnections")).isEqualTo("50");
		assertThat(getPropertyValueAsString(beanDefinition, "localMaxConnections")).isEqualTo("200");
		assertThat(getPropertyValueAsString(beanDefinition, "localMaxSimultaneousRequests")).isEqualTo("50");
		assertThat(getPropertyValueAsString(beanDefinition, "localMinSimultaneousRequests")).isEqualTo("5");
		assertThat(getPropertyValueAsString(beanDefinition, "remoteCoreConnections")).isNull();
		assertThat(getPropertyValueAsString(beanDefinition, "remoteMaxConnections")).isNull();
		assertThat(getPropertyValueAsString(beanDefinition, "remoteMaxSimultaneousRequests")).isNull();
		assertThat(getPropertyValueAsString(beanDefinition, "remoteMinSimultaneousRequests")).isNull();

		verify(mockElement, never()).getAttribute(eq("heartbeat-interval-seconds"));
		verify(mockElement, never()).getAttribute(eq("idle-timeout-seconds"));
		verify(mockElement, never()).getAttribute(eq("initialization-executor-ref"));
		verify(mockElement, never()).getAttribute(eq("pool-timeout-milliseconds"));
		verify(mockElement).getAttribute(eq("core-connections"));
		verify(mockElement).getAttribute(eq("max-connections"));
		verify(mockElement).getAttribute(eq("max-simultaneous-requests"));
		verify(mockElement).getAttribute(eq("min-simultaneous-requests"));
	}

	@Test // DATACASS-298
	public void parseRemotePoolingOptionsProperlyConfiguresBeanDefinition() {

		when(mockElement.getAttribute(eq("core-connections"))).thenReturn("50");
		when(mockElement.getAttribute(eq("max-connections"))).thenReturn("200");
		when(mockElement.getAttribute(eq("max-simultaneous-requests"))).thenReturn("50");
		when(mockElement.getAttribute(eq("min-simultaneous-requests"))).thenReturn("5");

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition();

		parser.parseRemotePoolingOptions(mockElement, builder);

		BeanDefinition beanDefinition = builder.getBeanDefinition();

		assertThat(getPropertyValueAsString(beanDefinition, "heartbeatIntervalSeconds")).isNull();
		assertThat(getPropertyValueAsString(beanDefinition, "idleTimeoutSeconds")).isNull();
		assertThat(getPropertyValueAsString(beanDefinition, "initializationExecutor")).isNull();
		assertThat(getPropertyValueAsString(beanDefinition, "poolTimeoutMilliseconds")).isNull();
		assertThat(getPropertyValueAsString(beanDefinition, "localCoreConnections")).isNull();
		assertThat(getPropertyValueAsString(beanDefinition, "localMaxConnections")).isNull();
		assertThat(getPropertyValueAsString(beanDefinition, "localMaxSimultaneousRequests")).isNull();
		assertThat(getPropertyValueAsString(beanDefinition, "localMinSimultaneousRequests")).isNull();
		assertThat(getPropertyValueAsString(beanDefinition, "remoteCoreConnections")).isEqualTo("50");
		assertThat(getPropertyValueAsString(beanDefinition, "remoteMaxConnections")).isEqualTo("200");
		assertThat(getPropertyValueAsString(beanDefinition, "remoteMaxSimultaneousRequests")).isEqualTo("50");
		assertThat(getPropertyValueAsString(beanDefinition, "remoteMinSimultaneousRequests")).isEqualTo("5");

		verify(mockElement, never()).getAttribute(eq("heartbeat-interval-seconds"));
		verify(mockElement, never()).getAttribute(eq("idle-timeout-seconds"));
		verify(mockElement, never()).getAttribute(eq("initialization-executor-ref"));
		verify(mockElement, never()).getAttribute(eq("pool-timeout-milliseconds"));
		verify(mockElement).getAttribute(eq("core-connections"));
		verify(mockElement).getAttribute(eq("max-connections"));
		verify(mockElement).getAttribute(eq("max-simultaneous-requests"));
		verify(mockElement).getAttribute(eq("min-simultaneous-requests"));
	}

	@Test // DATACASS-298
	public void parseScript() {

		when(mockElement.getTextContent()).thenReturn("CREATE TABLE schema.table;");
		assertThat(parser.parseScript(mockElement)).isEqualTo("CREATE TABLE schema.table;");
		verify(mockElement).getTextContent();
	}

	@Test // DATACASS-298
	public void newSocketOptionsBeanDefinitionIsProperlyInitialized() {

		when(mockElement.getAttribute(eq("connect-timeout-millis"))).thenReturn("15000");
		when(mockElement.getAttribute(eq("keep-alive"))).thenReturn("true");
		when(mockElement.getAttribute(eq("read-timeout-millis"))).thenReturn("20000");
		when(mockElement.getAttribute(eq("receive-buffer-size"))).thenReturn("32768");
		when(mockElement.getAttribute(eq("reuse-address"))).thenReturn("true");
		when(mockElement.getAttribute(eq("send-buffer-size"))).thenReturn("16384");
		when(mockElement.getAttribute(eq("so-linger"))).thenReturn("false");
		when(mockElement.getAttribute(eq("tcp-no-delay"))).thenReturn("true");

		BeanDefinition beanDefinition = parser.newSocketOptionsBeanDefinition(mockElement, mockParserContext(null));

		assertThat(beanDefinition).isNotNull();
		assertThat(beanDefinition.getBeanClassName()).isEqualTo(SocketOptionsFactoryBean.class.getName());
		assertThat((Element) beanDefinition.getSource()).isEqualTo(mockElement);
		assertThat(getPropertyValueAsString(beanDefinition, "connectTimeoutMillis")).isEqualTo("15000");
		assertThat(getPropertyValueAsString(beanDefinition, "keepAlive")).isEqualTo("true");
		assertThat(getPropertyValueAsString(beanDefinition, "readTimeoutMillis")).isEqualTo("20000");
		assertThat(getPropertyValueAsString(beanDefinition, "receiveBufferSize")).isEqualTo("32768");
		assertThat(getPropertyValueAsString(beanDefinition, "reuseAddress")).isEqualTo("true");
		assertThat(getPropertyValueAsString(beanDefinition, "sendBufferSize")).isEqualTo("16384");
		assertThat(getPropertyValueAsString(beanDefinition, "soLinger")).isEqualTo("false");
		assertThat(getPropertyValueAsString(beanDefinition, "tcpNoDelay")).isEqualTo("true");

		verify(mockElement).getAttribute(eq("connect-timeout-millis"));
		verify(mockElement).getAttribute(eq("keep-alive"));
		verify(mockElement).getAttribute(eq("read-timeout-millis"));
		verify(mockElement).getAttribute(eq("receive-buffer-size"));
		verify(mockElement).getAttribute(eq("reuse-address"));
		verify(mockElement).getAttribute(eq("send-buffer-size"));
		verify(mockElement).getAttribute(eq("so-linger"));
		verify(mockElement).getAttribute(eq("tcp-no-delay"));
	}

	private NodeList mockNodeList(Element... childElements) {

		NodeList mockNodeList = mock(NodeList.class);

		when(mockNodeList.getLength()).thenReturn(childElements.length);

		for (int index = 0; index < childElements.length; index++) {
			when(mockNodeList.item(eq(index))).thenReturn(childElements[index]);
		}

		return mockNodeList;
	}

	private ParserContext mockParserContext(BeanDefinition beanDefinition) {

		XmlReaderContext readerContext = new XmlReaderContext(null, null, null, new PassThroughSourceExtractor(), null,
				null);
		return new ParserContext(readerContext, new BeanDefinitionParserDelegate(readerContext), beanDefinition);
	}
}
