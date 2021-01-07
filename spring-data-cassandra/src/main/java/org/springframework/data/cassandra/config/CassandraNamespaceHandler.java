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

import org.springframework.beans.factory.xml.NamespaceHandlerSupport;

/**
 * Namespace handler for spring-data-cassandra.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @author Oliver Gierke
 */
public class CassandraNamespaceHandler extends NamespaceHandlerSupport {

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.xml.NamespaceHandler#init()
	 */
	@Override
	public void init() {

		registerBeanDefinitionParser("session", new CqlSessionParser());
		registerBeanDefinitionParser("session-factory", new SessionFactoryBeanDefinitionParser());
		registerBeanDefinitionParser("template", new CassandraTemplateParser());
		registerBeanDefinitionParser("cql-template", new CassandraCqlTemplateParser());
		registerBeanDefinitionParser("auditing", new CassandraAuditingBeanDefinitionParser());
		registerBeanDefinitionParser("converter", new CassandraMappingConverterParser());
		registerBeanDefinitionParser("mapping", new CassandraMappingContextParser());
		registerBeanDefinitionParser("initialize-keyspace", new InitializeKeyspaceBeanDefinitionParser());
	}
}
