/*
 * Copyright 2013-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.aot.BeanRegistrationAotProcessor;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.data.cassandra.config.DefaultBeanNames;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.aot.CassandraRepositoryContributor;
import org.springframework.data.cassandra.repository.support.CassandraRepositoryFactoryBean;
import org.springframework.data.cassandra.repository.support.SimpleCassandraRepository;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;
import org.springframework.data.repository.config.AotRepositoryContext;
import org.springframework.data.repository.config.RepositoryConfigurationExtension;
import org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport;
import org.springframework.data.repository.config.RepositoryRegistrationAotProcessor;
import org.springframework.data.repository.config.XmlRepositoryConfigurationSource;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.util.StringUtils;

import org.w3c.dom.Element;

/**
 * {@link RepositoryConfigurationExtension} for Cassandra.
 *
 * @author Alex Shvid
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Mateusz Szymczak
 * @author Chris Bono
 */
public class CassandraRepositoryConfigurationExtension extends RepositoryConfigurationExtensionSupport {

	private static final String CASSANDRA_TEMPLATE_REF = "cassandra-template-ref";

	@Override
	public String getModuleName() {
		return "Cassandra";
	}

	@Override
	protected String getModulePrefix() {
		return "cassandra";
	}

	@Override
	public String getRepositoryBaseClassName() {
		return SimpleCassandraRepository.class.getName();
	}

	@Override
	public String getRepositoryFactoryBeanClassName() {
		return CassandraRepositoryFactoryBean.class.getName();
	}

	@Override
	public Class<? extends BeanRegistrationAotProcessor> getRepositoryAotProcessor() {
		return CassandraRepositoryRegistrationAotProcessor.class;
	}

	@Override
	public void postProcess(BeanDefinitionBuilder builder, XmlRepositoryConfigurationSource config) {

		Element element = config.getElement();

		String cassandraTemplateRef = Optional.ofNullable(element.getAttribute(CASSANDRA_TEMPLATE_REF)) //
				.filter(StringUtils::hasText) //
				.orElse(DefaultBeanNames.DATA_TEMPLATE);

		builder.addPropertyReference("cassandraTemplate", cassandraTemplateRef);
	}

	@Override
	public void postProcess(BeanDefinitionBuilder builder, AnnotationRepositoryConfigurationSource config) {

		String cassandraTemplateRef = config.getAttributes().getString("cassandraTemplateRef");

		if (StringUtils.hasText(cassandraTemplateRef)) {
			builder.addPropertyReference("cassandraTemplate", cassandraTemplateRef);
		}
	}

	@Override
	protected Collection<Class<? extends Annotation>> getIdentifyingAnnotations() {
		return Collections.singleton(Table.class);
	}

	@Override
	protected Collection<Class<?>> getIdentifyingTypes() {
		return Collections.singleton(CassandraRepository.class);
	}

	@Override
	protected boolean useRepositoryConfiguration(RepositoryMetadata metadata) {
		return !metadata.isReactiveRepository();
	}

	/**
	 * Cassandra-specific {@link BeanRegistrationAotProcessor AOT processor}.
	 *
	 * @author Chris Bono
	 * @author Mark Paluch
	 * @since 5.0
	 */
	public static class CassandraRepositoryRegistrationAotProcessor extends RepositoryRegistrationAotProcessor {

		private static final String MODULE_NAME = "cassandra";

		protected @Nullable CassandraRepositoryContributor contributeAotRepository(AotRepositoryContext repositoryContext) {

			if (!repositoryContext.isGeneratedRepositoriesEnabled(MODULE_NAME)) {
				return null;
			}

			if(repositoryContext.getRepositoryInformation().isReactiveRepository()) {
				return null;
			}

			return new CassandraRepositoryContributor(repositoryContext);
		}

	}

}
