/*
 *  Copyright 2013-2016 the original author or authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.springframework.cassandra.config.xml;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.beans.PropertyValue;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.parsing.PassThroughSourceExtractor;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.BeanDefinitionParserDelegate;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.beans.factory.xml.XmlReaderContext;
import org.springframework.cassandra.config.CassandraCqlClusterFactoryBean;
import org.springframework.cassandra.config.PoolingOptionsFactoryBean;
import org.springframework.cassandra.config.SocketOptionsFactoryBean;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Test suite of Unit tests testing the contract and functionality of the {@link CassandraCqlClusterParser}.
 *
 * @author John Blum
 * @see org.springframework.cassandra.config.xml.CassandraCqlClusterParser
 * @since 1.5.0
 */
// TODO add more tests!
@RunWith(MockitoJUnitRunner.class)
public class CassandraCqlClusterParserUnitTests {

	@Mock
	private Element mockElement;

	private CassandraCqlClusterParser parser = new CassandraCqlClusterParser();

	@SuppressWarnings("unchecked")
	protected <T> T getPropertyValue(BeanDefinition beanDefinition, String propertyName) {
		PropertyValue propertyValue = beanDefinition.getPropertyValues().getPropertyValue(propertyName);

		return (T) (propertyValue != null ? propertyValue.getValue() : null);
	}

	protected String getPropertyValueAsString(BeanDefinition beanDefinition, String propertyName) {
		Object value = getPropertyValue(beanDefinition, propertyName);

		return (value instanceof RuntimeBeanReference ? ((RuntimeBeanReference) value).getBeanName()
			: (value != null ? String.valueOf(value) : null));
	}

	protected BeanDefinitionParserDelegate mockBeanDefinitionParserDelegate(XmlReaderContext xmlReaderContext) {
		return new BeanDefinitionParserDelegate(xmlReaderContext);
	}

	protected NodeList mockNodeList(Element... childElements) {
		NodeList mockNodeList = mock(NodeList.class);

		when(mockNodeList.getLength()).thenReturn(childElements.length);

		for (int index = 0; index < childElements.length; index++) {
			when(mockNodeList.item(eq(index))).thenReturn(childElements[index]);
		}

		return mockNodeList;
	}

	protected ParserContext mockParserContext() {
		return mockParserContext(null);
	}

	protected ParserContext mockParserContext(BeanDefinition beanDefinition) {
		XmlReaderContext readerContext = mockXmlReaderContext();
		return new ParserContext(readerContext, mockBeanDefinitionParserDelegate(readerContext), beanDefinition);
	}

	protected XmlReaderContext mockXmlReaderContext() {
		return new XmlReaderContext(null, null, null, new PassThroughSourceExtractor(), null, null);
	}

	@Test
	public void resolveIdFromElement() {
		when(mockElement.getAttribute(eq(CassandraCqlClusterParser.ID_ATTRIBUTE))).thenReturn("test");
		assertThat(parser.resolveId(mockElement, null, null), is(equalTo("test")));
		verify(mockElement, times(1)).getAttribute(eq(CassandraCqlClusterParser.ID_ATTRIBUTE));
	}

	@Test
	public void resolveIdUsingDefault() {
		when(mockElement.getAttribute(eq(CassandraCqlClusterParser.ID_ATTRIBUTE))).thenReturn("");
		assertThat(parser.resolveId(mockElement, null, null), is(equalTo(DefaultCqlBeanNames.CLUSTER)));
		verify(mockElement, times(1)).getAttribute(eq(CassandraCqlClusterParser.ID_ATTRIBUTE));
	}

	@Test
	public void parseInternalCallsDoParseAndConstructsBeanDefinition() {
		BeanDefinition mockContainingBeanDefinition = mock(BeanDefinition.class);

		when(mockContainingBeanDefinition.getScope()).thenReturn("Singleton");
		when(mockElement.getAttribute("auth-info-provider-ref")).thenReturn("testAuthInfoProvider");
		when(mockElement.getAttribute("host-state-listener-ref")).thenReturn("testHostStateListener");
		when(mockElement.getAttribute("latency-tracker-ref")).thenReturn("testLatencyTracker");
		when(mockElement.getAttribute("load-balancing-policy-ref")).thenReturn("testLoadBalancingPolicy");
		when(mockElement.getAttribute("reconnection-policy-ref")).thenReturn("testReconnectionPolicy");
		when(mockElement.getAttribute("retry-policy-ref")).thenReturn("testRetryPolicy");
		when(mockElement.getAttribute("ssl-options-ref")).thenReturn("testSslOptions");
		when(mockElement.getAttribute("contact-points")).thenReturn("skullbox");
		when(mockElement.getAttribute("compression")).thenReturn("SNAPPY");
		when(mockElement.getAttribute("jmx-reporting-enabled")).thenReturn("true");
		when(mockElement.getAttribute("metrics-enabled")).thenReturn("true");
		when(mockElement.getAttribute("password")).thenReturn("p@55w0rd");
		when(mockElement.getAttribute("port")).thenReturn("12345");
		when(mockElement.getAttribute("ssl-enabled")).thenReturn("true");
		when(mockElement.getAttribute("username")).thenReturn("jonDoe");

		CassandraCqlClusterParser parser = new CassandraCqlClusterParser() {
			@Override
			protected void parseChildElements(Element element, ParserContext parserContext,
				BeanDefinitionBuilder builder) {
			}
		};

		AbstractBeanDefinition beanDefinition = parser.parseInternal(mockElement, mockParserContext(
			mockContainingBeanDefinition));

		assertThat(beanDefinition, is(notNullValue(BeanDefinition.class)));
		assertThat(beanDefinition.getBeanClassName(), is(equalTo(CassandraCqlClusterFactoryBean.class.getName())));
		assertThat(beanDefinition.getDestroyMethodName(), is(equalTo("destroy")));
		assertThat((Element) beanDefinition.getSource(), is(equalTo(mockElement)));
		assertThat(beanDefinition.isLazyInit(), is(false));
		assertThat(getPropertyValueAsString(beanDefinition, "authProvider"), is(equalTo("testAuthInfoProvider")));
		assertThat(getPropertyValueAsString(beanDefinition, "hostStateListener"), is(equalTo("testHostStateListener")));
		assertThat(getPropertyValueAsString(beanDefinition, "latencyTracker"), is(equalTo("testLatencyTracker")));
		assertThat(getPropertyValueAsString(beanDefinition, "loadBalancingPolicy"), is(equalTo("testLoadBalancingPolicy")));
		assertThat(getPropertyValueAsString(beanDefinition, "reconnectionPolicy"), is(equalTo("testReconnectionPolicy")));
		assertThat(getPropertyValueAsString(beanDefinition, "retryPolicy"), is(equalTo("testRetryPolicy")));
		assertThat(getPropertyValueAsString(beanDefinition, "sslOptions"), is(equalTo("testSslOptions")));
		assertThat(getPropertyValueAsString(beanDefinition, "contactPoints"), is(equalTo("skullbox")));
		assertThat(getPropertyValueAsString(beanDefinition, "compressionType"), is(equalTo("SNAPPY")));
		assertThat(getPropertyValueAsString(beanDefinition, "jmxReportingEnabled"), is(equalTo("true")));
		assertThat(getPropertyValueAsString(beanDefinition, "metricsEnabled"), is(equalTo("true")));
		assertThat(getPropertyValueAsString(beanDefinition, "password"), is(equalTo("p@55w0rd")));
		assertThat(getPropertyValueAsString(beanDefinition, "port"), is(equalTo("12345")));
		assertThat(getPropertyValueAsString(beanDefinition, "sslEnabled"), is(equalTo("true")));
		assertThat(getPropertyValueAsString(beanDefinition, "username"), is(equalTo("jonDoe")));

		verify(mockContainingBeanDefinition, times(1)).getScope();
		verify(mockElement, times(1)).getAttribute(eq("auth-info-provider-ref"));
		verify(mockElement, times(1)).getAttribute(eq("host-state-listener-ref"));
		verify(mockElement, times(1)).getAttribute(eq("latency-tracker-ref"));
		verify(mockElement, times(1)).getAttribute(eq("load-balancing-policy-ref"));
		verify(mockElement, times(1)).getAttribute(eq("reconnection-policy-ref"));
		verify(mockElement, times(1)).getAttribute(eq("retry-policy-ref"));
		verify(mockElement, times(1)).getAttribute(eq("ssl-options-ref"));
		verify(mockElement, times(1)).getAttribute(eq("contact-points"));
		verify(mockElement, times(1)).getAttribute(eq("compression"));
		verify(mockElement, times(1)).getAttribute(eq("jmx-reporting-enabled"));
		verify(mockElement, times(1)).getAttribute(eq("metrics-enabled"));
		verify(mockElement, times(1)).getAttribute(eq("password"));
		verify(mockElement, times(1)).getAttribute(eq("port"));
		verify(mockElement, times(1)).getAttribute(eq("ssl-enabled"));
		verify(mockElement, times(1)).getAttribute(eq("username"));
	}

	@Test
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

		parser.parseChildElements(mockElement, mockParserContext(), builder);

		BeanDefinition beanDefinition = builder.getBeanDefinition();

		BeanDefinition poolingOptionsBeanDefinition = getPropertyValue(beanDefinition, "poolingOptions");

		assertThat(poolingOptionsBeanDefinition, is(notNullValue(BeanDefinition.class)));
		assertThat(poolingOptionsBeanDefinition.getBeanClassName(), is(equalTo(PoolingOptionsFactoryBean.class.getName())));
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "heartbeatIntervalSeconds"), is(equalTo("15")));
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "idleTimeoutSeconds"), is(equalTo("120")));
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "initializationExecutor"), is(equalTo("testExecutor")));
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "poolTimeoutMilliseconds"), is(equalTo("60000")));
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "localCoreConnections"), is(equalTo("50")));
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "localMaxConnections"), is(equalTo("200")));
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "localMaxSimultaneousRequests"), is(equalTo("50")));
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "localMinSimultaneousRequests"), is(equalTo("5")));
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "remoteCoreConnections"), is(nullValue()));
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "remoteMaxConnections"), is(nullValue()));
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "remoteMaxSimultaneousRequests"), is(nullValue()));
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "remoteMinSimultaneousRequests"), is(nullValue()));

		verify(mockElement, times(1)).getChildNodes();
		verify(mockElement, times(1)).getAttribute(eq("heartbeat-interval-seconds"));
		verify(mockElement, times(1)).getAttribute(eq("idle-timeout-seconds"));
		verify(mockElement, times(1)).getAttribute(eq("initialization-executor-ref"));
		verify(mockElement, times(1)).getAttribute(eq("pool-timeout-milliseconds"));
		verify(localPoolingOptionsElement, times(1)).getLocalName();
		verify(localPoolingOptionsElement, times(1)).getAttribute(eq("core-connections"));
		verify(localPoolingOptionsElement, times(1)).getAttribute(eq("max-connections"));
		verify(localPoolingOptionsElement, times(1)).getAttribute(eq("max-simultaneous-requests"));
		verify(localPoolingOptionsElement, times(1)).getAttribute(eq("min-simultaneous-requests"));
	}

	@Test
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

		parser.parseChildElements(mockElement, mockParserContext(), builder);

		BeanDefinition beanDefinition = builder.getBeanDefinition();

		BeanDefinition poolingOptionsBeanDefinition = getPropertyValue(beanDefinition, "poolingOptions");

		assertThat(poolingOptionsBeanDefinition, is(notNullValue(BeanDefinition.class)));
		assertThat(poolingOptionsBeanDefinition.getBeanClassName(), is(equalTo(PoolingOptionsFactoryBean.class.getName())));
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "heartbeatIntervalSeconds"), is(equalTo("15")));
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "idleTimeoutSeconds"), is(equalTo("120")));
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "initializationExecutor"), is(equalTo("testExecutor")));
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "poolTimeoutMilliseconds"), is(equalTo("60000")));
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "localCoreConnections"), is(nullValue()));
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "localMaxConnections"), is(nullValue()));
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "localMaxSimultaneousRequests"), is(nullValue()));
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "localMinSimultaneousRequests"), is(nullValue()));
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "remoteCoreConnections"), is(equalTo("50")));
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "remoteMaxConnections"), is(equalTo("200")));
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "remoteMaxSimultaneousRequests"), is(equalTo(
			"50")));
		assertThat(getPropertyValueAsString(poolingOptionsBeanDefinition, "remoteMinSimultaneousRequests"), is(equalTo(
			"5")));

		verify(mockElement, times(1)).getChildNodes();
		verify(mockElement, times(1)).getAttribute(eq("heartbeat-interval-seconds"));
		verify(mockElement, times(1)).getAttribute(eq("idle-timeout-seconds"));
		verify(mockElement, times(1)).getAttribute(eq("initialization-executor-ref"));
		verify(mockElement, times(1)).getAttribute(eq("pool-timeout-milliseconds"));
		verify(localPoolingOptionsElement, times(1)).getLocalName();
		verify(localPoolingOptionsElement, times(1)).getAttribute(eq("core-connections"));
		verify(localPoolingOptionsElement, times(1)).getAttribute(eq("max-connections"));
		verify(localPoolingOptionsElement, times(1)).getAttribute(eq("max-simultaneous-requests"));
		verify(localPoolingOptionsElement, times(1)).getAttribute(eq("min-simultaneous-requests"));
	}

	@Test
	public void parseLocalPoolingOptionsProperlyConfiguresBeanDefinition() {
		when(mockElement.getAttribute(eq("core-connections"))).thenReturn("50");
		when(mockElement.getAttribute(eq("max-connections"))).thenReturn("200");
		when(mockElement.getAttribute(eq("max-simultaneous-requests"))).thenReturn("50");
		when(mockElement.getAttribute(eq("min-simultaneous-requests"))).thenReturn("5");

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition();

		parser.parseLocalPoolingOptions(mockElement, builder);

		BeanDefinition beanDefinition = builder.getBeanDefinition();

		assertThat(getPropertyValueAsString(beanDefinition, "heartbeatIntervalSeconds"), is(nullValue()));
		assertThat(getPropertyValueAsString(beanDefinition, "idleTimeoutSeconds"), is(nullValue()));
		assertThat(getPropertyValueAsString(beanDefinition, "initializationExecutor"), is(nullValue()));
		assertThat(getPropertyValueAsString(beanDefinition, "poolTimeoutMilliseconds"), is(nullValue()));
		assertThat(getPropertyValueAsString(beanDefinition, "localCoreConnections"), is(equalTo("50")));
		assertThat(getPropertyValueAsString(beanDefinition, "localMaxConnections"), is(equalTo("200")));
		assertThat(getPropertyValueAsString(beanDefinition, "localMaxSimultaneousRequests"), is(equalTo("50")));
		assertThat(getPropertyValueAsString(beanDefinition, "localMinSimultaneousRequests"), is(equalTo("5")));
		assertThat(getPropertyValueAsString(beanDefinition, "remoteCoreConnections"), is(nullValue()));
		assertThat(getPropertyValueAsString(beanDefinition, "remoteMaxConnections"), is(nullValue()));
		assertThat(getPropertyValueAsString(beanDefinition, "remoteMaxSimultaneousRequests"), is(nullValue()));
		assertThat(getPropertyValueAsString(beanDefinition, "remoteMinSimultaneousRequests"), is(nullValue()));

		verify(mockElement, never()).getAttribute(eq("heartbeat-interval-seconds"));
		verify(mockElement, never()).getAttribute(eq("idle-timeout-seconds"));
		verify(mockElement, never()).getAttribute(eq("initialization-executor-ref"));
		verify(mockElement, never()).getAttribute(eq("pool-timeout-milliseconds"));
		verify(mockElement, times(1)).getAttribute(eq("core-connections"));
		verify(mockElement, times(1)).getAttribute(eq("max-connections"));
		verify(mockElement, times(1)).getAttribute(eq("max-simultaneous-requests"));
		verify(mockElement, times(1)).getAttribute(eq("min-simultaneous-requests"));
	}

	@Test
	public void parseRemotePoolingOptionsProperlyConfiguresBeanDefinition() {
		when(mockElement.getAttribute(eq("core-connections"))).thenReturn("50");
		when(mockElement.getAttribute(eq("max-connections"))).thenReturn("200");
		when(mockElement.getAttribute(eq("max-simultaneous-requests"))).thenReturn("50");
		when(mockElement.getAttribute(eq("min-simultaneous-requests"))).thenReturn("5");

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition();

		parser.parseRemotePoolingOptions(mockElement, builder);

		BeanDefinition beanDefinition = builder.getBeanDefinition();

		assertThat(getPropertyValueAsString(beanDefinition, "heartbeatIntervalSeconds"), is(nullValue()));
		assertThat(getPropertyValueAsString(beanDefinition, "idleTimeoutSeconds"), is(nullValue()));
		assertThat(getPropertyValueAsString(beanDefinition, "initializationExecutor"), is(nullValue()));
		assertThat(getPropertyValueAsString(beanDefinition, "poolTimeoutMilliseconds"), is(nullValue()));
		assertThat(getPropertyValueAsString(beanDefinition, "localCoreConnections"), is(nullValue()));
		assertThat(getPropertyValueAsString(beanDefinition, "localMaxConnections"), is(nullValue()));
		assertThat(getPropertyValueAsString(beanDefinition, "localMaxSimultaneousRequests"), is(nullValue()));
		assertThat(getPropertyValueAsString(beanDefinition, "localMinSimultaneousRequests"), is(nullValue()));
		assertThat(getPropertyValueAsString(beanDefinition, "remoteCoreConnections"), is(equalTo("50")));
		assertThat(getPropertyValueAsString(beanDefinition, "remoteMaxConnections"), is(equalTo("200")));
		assertThat(getPropertyValueAsString(beanDefinition, "remoteMaxSimultaneousRequests"), is(equalTo("50")));
		assertThat(getPropertyValueAsString(beanDefinition, "remoteMinSimultaneousRequests"), is(equalTo("5")));

		verify(mockElement, never()).getAttribute(eq("heartbeat-interval-seconds"));
		verify(mockElement, never()).getAttribute(eq("idle-timeout-seconds"));
		verify(mockElement, never()).getAttribute(eq("initialization-executor-ref"));
		verify(mockElement, never()).getAttribute(eq("pool-timeout-milliseconds"));
		verify(mockElement, times(1)).getAttribute(eq("core-connections"));
		verify(mockElement, times(1)).getAttribute(eq("max-connections"));
		verify(mockElement, times(1)).getAttribute(eq("max-simultaneous-requests"));
		verify(mockElement, times(1)).getAttribute(eq("min-simultaneous-requests"));
	}

	@Test
	public void parseScript() {
		when(mockElement.getTextContent()).thenReturn("CREATE TABLE schema.table;");
		assertThat(parser.parseScript(mockElement), is(equalTo("CREATE TABLE schema.table;")));
		verify(mockElement, times(1)).getTextContent();
	}

	@Test
	public void newSocketOptionsBeanDefinitionIsProperlyInitialized() {
		when(mockElement.getAttribute(eq("connect-timeout-mls"))).thenReturn("5000");
		when(mockElement.getAttribute(eq("keep-alive"))).thenReturn("true");
		when(mockElement.getAttribute(eq("read-timeout-mls"))).thenReturn("30000");
		when(mockElement.getAttribute(eq("receive-buffer-size"))).thenReturn("32768");
		when(mockElement.getAttribute(eq("reuse-address"))).thenReturn("true");
		when(mockElement.getAttribute(eq("send-buffer-size"))).thenReturn("16384");
		when(mockElement.getAttribute(eq("so-linger"))).thenReturn("false");
		when(mockElement.getAttribute(eq("tcp-no-delay"))).thenReturn("true");

		BeanDefinition beanDefinition = parser.newSocketOptionsBeanDefinition(mockElement, mockParserContext());

		assertThat(beanDefinition, is(notNullValue(BeanDefinition.class)));
		assertThat(beanDefinition.getBeanClassName(), is(equalTo(SocketOptionsFactoryBean.class.getName())));
		assertThat((Element) beanDefinition.getSource(), is(equalTo(mockElement)));
		assertThat(getPropertyValueAsString(beanDefinition, "connectTimeoutMillis"), is(equalTo("5000")));
		assertThat(getPropertyValueAsString(beanDefinition, "keepAlive"), is(equalTo("true")));
		assertThat(getPropertyValueAsString(beanDefinition, "readTimeoutMillis"), is(equalTo("30000")));
		assertThat(getPropertyValueAsString(beanDefinition, "receiveBufferSize"), is(equalTo("32768")));
		assertThat(getPropertyValueAsString(beanDefinition, "reuseAddress"), is(equalTo("true")));
		assertThat(getPropertyValueAsString(beanDefinition, "sendBufferSize"), is(equalTo("16384")));
		assertThat(getPropertyValueAsString(beanDefinition, "soLinger"), is(equalTo("false")));
		assertThat(getPropertyValueAsString(beanDefinition, "tcpNoDelay"), is(equalTo("true")));

		verify(mockElement, times(1)).getAttribute(eq("connect-timeout-mls"));
		verify(mockElement, times(1)).getAttribute(eq("keep-alive"));
		verify(mockElement, times(1)).getAttribute(eq("read-timeout-mls"));
		verify(mockElement, times(1)).getAttribute(eq("receive-buffer-size"));
		verify(mockElement, times(1)).getAttribute(eq("reuse-address"));
		verify(mockElement, times(1)).getAttribute(eq("send-buffer-size"));
		verify(mockElement, times(1)).getAttribute(eq("so-linger"));
		verify(mockElement, times(1)).getAttribute(eq("tcp-no-delay"));
	}
}
