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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSimpleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.cassandra.config.CassandraClusterFactoryBean;
import org.springframework.cassandra.config.CompressionType;
import org.springframework.cassandra.config.PoolingOptionsConfig;
import org.springframework.cassandra.config.SocketOptionsConfig;
import org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.DefaultOption;
import org.springframework.cassandra.core.keyspace.DropKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.KeyspaceOption;
import org.springframework.cassandra.core.keyspace.Option;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Parser for &lt;cluster;gt; definitions.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 */

public class CassandraClusterParser extends AbstractSimpleBeanDefinitionParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return CassandraClusterFactoryBean.class;
	}

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {

		String id = super.resolveId(element, definition, parserContext);
		return StringUtils.hasText(id) ? id : BeanNames.CASSANDRA_CLUSTER;
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

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
			builder.addPropertyValue("compressionType", CompressionType.valueOf(compression));
		}

		parseChildElements(builder, element);
	}

	protected void parseChildElements(BeanDefinitionBuilder builder, Element element) {

		List<CreateKeyspaceSpecification> creates = new ArrayList<CreateKeyspaceSpecification>();
		List<DropKeyspaceSpecification> drops = new ArrayList<DropKeyspaceSpecification>();
		List<String> scripts = new ArrayList<String>();

		List<Element> elements = DomUtils.getChildElements(element);

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

				KeyspaceSpecifications specifications = parseKeyspace(subElement);

				if (specifications.create != null) {
					creates.add(specifications.create);
				}
				if (specifications.drop != null) {
					drops.add(specifications.drop);
				}
			} else if ("cql".equals(name)) {
				scripts.add(parseScript(subElement));
			}
		}

		builder.addPropertyValue("keyspaceCreations", creates);
		builder.addPropertyValue("keyspaceDrops", drops);
		builder.addPropertyValue("scripts", scripts);
	}

	private KeyspaceSpecifications parseKeyspace(Element element) {

		CreateKeyspaceSpecification create = null;
		DropKeyspaceSpecification drop = null;

		String name = element.getAttribute("name");
		if (name == null || name.trim().length() == 0) {
			name = BeanNames.CASSANDRA_KEYSPACE;
		}

		boolean durableWrites = Boolean.valueOf(element.getAttribute("durable-writes"));

		String action = element.getAttribute("action");
		if (action == null || action.trim().length() == 0) {
			throw new IllegalArgumentException("attribute action must be given");
		}

		if (action.startsWith("CREATE")) {

			create = CreateKeyspaceSpecification.createKeyspace().name(name)
					.with(KeyspaceOption.DURABLE_WRITES, durableWrites);

			NodeList nodes = element.getElementsByTagName("replication");
			parseReplication((Element) (nodes.getLength() == 1 ? nodes.item(0) : null), create);
		}

		if (action.equals("CREATE-DROP")) {
			drop = DropKeyspaceSpecification.dropKeyspace().name(create.getName());
		}

		return new KeyspaceSpecifications(create, drop);
	}

	protected void parseReplication(Element element, CreateKeyspaceSpecification create) {

		String strategyClass = null;
		if (element != null) {
			strategyClass = element.getAttribute("class");
		}
		if (strategyClass == null || strategyClass.trim().length() == 0) {
			strategyClass = "SimpleStrategy";
		}

		Long replicationFactor = null;
		if (element != null) {
			String s = element.getAttribute("replication-factor");
			replicationFactor = (s == null || s.trim().length() == 0) ? null : Long.parseLong(s);
		}
		if (replicationFactor == null) {
			replicationFactor = 1L;
		}

		Map<Option, Object> replicationMap = new HashMap<Option, Object>();
		replicationMap.put(new DefaultOption("class", String.class, false, false, true), strategyClass);
		replicationMap.put(new DefaultOption("replication_factor", Long.class, true, false, false), replicationFactor);

		if (element != null) {

			NodeList dataCenters = element.getElementsByTagName("data-center");

			int length = dataCenters.getLength();
			for (int i = 0; i < length; i++) {

				Element dataCenter = (Element) dataCenters.item(i);

				replicationMap.put(new DefaultOption(dataCenter.getAttribute("name"), Long.class, false, false, true),
						dataCenter.getAttribute("replicas-per-node"));
			}
		}

		create.with(KeyspaceOption.REPLICATION, replicationMap);
	}

	private String parseScript(Element element) {
		return element.getTextContent();
	}

	private BeanDefinition parsePoolingOptions(Element element) {
		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(PoolingOptionsConfig.class);

		ParsingUtils.setPropertyValue(builder, element, "min-simultaneous-requests", "minSimultaneousRequests");
		ParsingUtils.setPropertyValue(builder, element, "max-simultaneous-requests", "maxSimultaneousRequests");
		ParsingUtils.setPropertyValue(builder, element, "core-connections", "coreConnections");
		ParsingUtils.setPropertyValue(builder, element, "max-connections", "maxConnections");

		return builder.getBeanDefinition();
	}

	private BeanDefinition parseSocketOptions(Element element) {
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

	private static class KeyspaceSpecifications {

		public KeyspaceSpecifications(CreateKeyspaceSpecification create, DropKeyspaceSpecification drop) {
			this.create = create;
			this.drop = drop;
		}

		public CreateKeyspaceSpecification create;
		public DropKeyspaceSpecification drop;
		// TODO: public AlterKeyspaceSpecification alter;
	}
}
