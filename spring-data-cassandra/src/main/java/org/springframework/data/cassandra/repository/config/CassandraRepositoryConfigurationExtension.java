/*
 * Copyright 2013-2016 the original author or authors
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

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.cassandra.config.xml.ParsingUtils;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.cassandra.config.DefaultBeanNames;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.support.CassandraRepositoryFactoryBean;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;
import org.springframework.data.repository.config.RepositoryConfiguration;
import org.springframework.data.repository.config.RepositoryConfigurationExtension;
import org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport;
import org.springframework.data.repository.config.RepositoryConfigurationSource;
import org.springframework.data.repository.config.XmlRepositoryConfigurationSource;
import org.springframework.data.repository.query.ReactiveWrappers;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * {@link RepositoryConfigurationExtension} for Cassandra.
 * 
 * @author Alex Shvid
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class CassandraRepositoryConfigurationExtension extends RepositoryConfigurationExtensionSupport {

	private static final String CASSANDRA_TEMPLATE_REF = "cassandra-template-ref";

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport#getModuleName()
	 */
	@Override
	public String getModuleName() {
		return "Reactive Cassandra";
	}

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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport#getIdentifyingAnnotations()
	 */
	@Override
	protected Collection<Class<? extends Annotation>> getIdentifyingAnnotations() {
		return Collections.<Class<? extends Annotation>> singleton(Table.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport#getIdentifyingTypes()
	 */
	@Override
	protected Collection<Class<?>> getIdentifyingTypes() {
		return Collections.<Class<?>> singleton(CassandraRepository.class);
	}

	@Override
	public <T extends RepositoryConfigurationSource> Collection<RepositoryConfiguration<T>> getRepositoryConfigurations(
			T configSource, ResourceLoader loader, boolean strictMatchesOnly) {

		Collection<RepositoryConfiguration<T>> repositoryConfigurations = super.getRepositoryConfigurations(configSource,
				loader, strictMatchesOnly);

		if (ReactiveWrappers.isAvailable()) {

			return repositoryConfigurations.stream().filter(configuration -> {

				Class<?> repositoryInterface = super.loadRepositoryInterface(configuration, loader);
				return !RepositoryType.isReactiveRepository(repositoryInterface);

			}).collect(Collectors.toList());
		}

		return repositoryConfigurations;
	}

}
