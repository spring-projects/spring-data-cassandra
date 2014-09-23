/*
 * Copyright 2013-2014 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.repository.config;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.cassandra.config.xml.ParsingUtils;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.data.cassandra.config.DefaultBeanNames;
import org.springframework.data.cassandra.repository.support.CassandraRepositoryFactoryBean;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;
import org.springframework.data.repository.config.RepositoryConfigurationExtension;
import org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport;
import org.springframework.data.repository.config.XmlRepositoryConfigurationSource;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * {@link RepositoryConfigurationExtension} for Cassandra.
 * 
 * @author Alex Shvid
 */
public class CassandraRepositoryConfigurationExtension extends RepositoryConfigurationExtensionSupport {

	private static final String CASSANDRA_TEMPLATE_REF = "cassandra-template-ref";

	@Override
	protected String getModulePrefix() {
		return "cassandra";
	}

	@Override
	public String getRepositoryFactoryClassName() {
		return CassandraRepositoryFactoryBean.class.getName();
	}

	@Override
	public void postProcess(BeanDefinitionBuilder builder, XmlRepositoryConfigurationSource config) {

		Element element = config.getElement();

		ParsingUtils.addOptionalPropertyReference(builder, "cassandraTemplate", element, CASSANDRA_TEMPLATE_REF,
				DefaultBeanNames.TEMPLATE);
	}

	@Override
	public void postProcess(BeanDefinitionBuilder builder, AnnotationRepositoryConfigurationSource config) {

		AnnotationAttributes attributes = config.getAttributes();

		String cassandraTemplateRef = attributes.getString("cassandraTemplateRef");
		if (StringUtils.hasText(cassandraTemplateRef)) {
			builder.addPropertyReference("cassandraTemplate", cassandraTemplateRef);
		}
	}
}
