/*
 * Copyright 2019 the original author or authors.
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

import java.lang.annotation.Annotation;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.auditing.config.AuditingBeanDefinitionRegistrarSupport;
import org.springframework.data.auditing.config.AuditingConfiguration;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.mapping.event.AuditingEntityCallback;
import org.springframework.data.cassandra.core.mapping.event.ReactiveAuditingEntityCallback;
import org.springframework.data.config.ParsingUtils;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * {@link ImportBeanDefinitionRegistrar} to enable {@link EnableCassandraAuditing} annotation.
 *
 * @author Mark Paluch
 * @since 2.2
 */
class CassandraAuditingRegistrar extends AuditingBeanDefinitionRegistrarSupport {

	private static boolean PROJECT_REACTOR_AVAILABLE = ClassUtils.isPresent("reactor.core.publisher.Mono",
			CassandraAuditingRegistrar.class.getClassLoader());

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.auditing.config.AuditingBeanDefinitionRegistrarSupport#getAnnotation()
	 */
	@Override
	protected Class<? extends Annotation> getAnnotation() {
		return EnableCassandraAuditing.class;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.auditing.config.AuditingBeanDefinitionRegistrarSupport#getAuditingHandlerBeanName()
	 */
	@Override
	protected String getAuditingHandlerBeanName() {
		return "cassandraAuditingHandler";
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.auditing.config.AuditingBeanDefinitionRegistrarSupport#registerBeanDefinitions(org.springframework.core.type.AnnotationMetadata, org.springframework.beans.factory.support.BeanDefinitionRegistry)
	 */
	@Override
	public void registerBeanDefinitions(AnnotationMetadata annotationMetadata, BeanDefinitionRegistry registry) {

		Assert.notNull(annotationMetadata, "AnnotationMetadata must not be null!");
		Assert.notNull(registry, "BeanDefinitionRegistry must not be null!");

		super.registerBeanDefinitions(annotationMetadata, registry);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.auditing.config.AuditingBeanDefinitionRegistrarSupport#getAuditHandlerBeanDefinitionBuilder(org.springframework.data.auditing.config.AuditingConfiguration)
	 */
	@Override
	protected BeanDefinitionBuilder getAuditHandlerBeanDefinitionBuilder(AuditingConfiguration configuration) {

		Assert.notNull(configuration, "AuditingConfiguration must not be null!");

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(IsNewAwareAuditingHandler.class);

		BeanDefinitionBuilder definition = BeanDefinitionBuilder.genericBeanDefinition(CassandraMappingContextLookup.class);
		definition.setAutowireMode(AbstractBeanDefinition.AUTOWIRE_CONSTRUCTOR);

		builder.addConstructorArgValue(definition.getBeanDefinition());
		return configureDefaultAuditHandlerAttributes(configuration, builder);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.auditing.config.AuditingBeanDefinitionRegistrarSupport#registerAuditListener(org.springframework.beans.factory.config.BeanDefinition, org.springframework.beans.factory.support.BeanDefinitionRegistry)
	 */
	@Override
	protected void registerAuditListenerBeanDefinition(BeanDefinition auditingHandlerDefinition,
			BeanDefinitionRegistry registry) {

		Assert.notNull(auditingHandlerDefinition, "BeanDefinition must not be null!");
		Assert.notNull(registry, "BeanDefinitionRegistry must not be null!");

		BeanDefinitionBuilder listenerBeanDefinitionBuilder = BeanDefinitionBuilder
				.rootBeanDefinition(AuditingEntityCallback.class);
		listenerBeanDefinitionBuilder
				.addConstructorArgValue(ParsingUtils.getObjectFactoryBeanDefinition(getAuditingHandlerBeanName(), registry));

		registerInfrastructureBeanWithId(listenerBeanDefinitionBuilder.getBeanDefinition(),
				AuditingEntityCallback.class.getName(), registry);

		if (PROJECT_REACTOR_AVAILABLE) {
			registerReactiveAuditingEntityCallback(registry, auditingHandlerDefinition.getSource());
		}
	}

	private void registerReactiveAuditingEntityCallback(BeanDefinitionRegistry registry, Object source) {

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(ReactiveAuditingEntityCallback.class);

		builder.addConstructorArgValue(ParsingUtils.getObjectFactoryBeanDefinition(getAuditingHandlerBeanName(), registry));
		builder.getRawBeanDefinition().setSource(source);

		registerInfrastructureBeanWithId(builder.getBeanDefinition(), ReactiveAuditingEntityCallback.class.getName(),
				registry);
	}

	/**
	 * Simple helper to be able to wire the {@link MappingContext} from a
	 * {@link org.springframework.data.cassandra.core.convert.MappingCassandraConverter} bean available in the application
	 * context.
	 */
	static class CassandraMappingContextLookup
			implements FactoryBean<MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty>> {

		private final MappingCassandraConverter converter;

		/**
		 * Creates a new {@link CassandraMappingContextLookup} for the given {@link MappingCassandraConverter}.
		 *
		 * @param converter must not be {@literal null}.
		 */
		public CassandraMappingContextLookup(MappingCassandraConverter converter) {
			this.converter = converter;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.beans.factory.FactoryBean#getObject()
		 */
		@Override
		public MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> getObject()
				throws Exception {
			return converter.getMappingContext();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
		 */
		@Override
		public Class<?> getObjectType() {
			return MappingContext.class;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.beans.factory.FactoryBean#isSingleton()
		 */
		@Override
		public boolean isSingleton() {
			return true;
		}
	}
}
