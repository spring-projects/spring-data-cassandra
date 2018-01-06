/*
 * Copyright 2013-2018 the original author or authors.
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
package org.springframework.data.cassandra.config;

import static org.springframework.data.cassandra.config.BeanDefinitionUtils.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.Session;

/**
 * {@link BeanFactoryPostProcessor} that does its best to register any missing Spring Data Cassandra beans that can be
 * defaulted. Specifically, it attempts to create default bean definitions for the following required interface types
 * via their default implementation types:
 * <ul>
 * <li>{@link CassandraOperations} via {@link CassandraTemplate}</li>
 * <li>{@link CassandraMappingContext} via {@link CassandraMappingContext}</li>
 * <li>{@link CassandraConverter} via {@link MappingCassandraConverter}</li>
 * </ul>
 * <p/>
 * If there are multiple definitions for any type that another type depends on, an {@link IllegalStateException} is
 * thrown. For example, if there are two definitions for type {@link CassandraMappingContext} present and no definition
 * for type {@link CassandraConverter}, then it's impossible to know which {@link CassandraMappingContext} is to be used
 * when creating a default definition for the {@link CassandraConverter}.
 * <p/>
 * If a single definition of a required type is present, then it is used. For example, if there is already a
 * {@link CassandraMappingContext} definition present, then it will be used in the {@link CassandraMappingContext} bean
 * definition.
 * <p/>
 * It requires that a single {@link Session} or {@link CassandraSessionFactoryBean} definition be present. As described
 * above, multiple {@link Session} definitions, multiple {@link CassandraSessionFactoryBean} definitions, or both a
 * {@link Session} and {@link CassandraSessionFactoryBean} will cause an {@link IllegalStateException} to be thrown.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @author Mateusz Szymczak
 * @deprecated Will be removed with the next major release. Spring Data Cassandra is not the best place to apply
 *             configuration defaults.
 */
@Deprecated
public class CassandraMappingBeanFactoryPostProcessor implements BeanFactoryPostProcessor {

	/**
	 * Ensures that {@link BeanDefinition}s for a {@link CassandraMappingContext} and a {@link CassandraConverter} exist.
	 */
	@Override
	public void postProcessBeanFactory(ConfigurableListableBeanFactory factory) throws BeansException {

		if (!(factory instanceof BeanDefinitionRegistry)) {
			return;
		}

		registerMissingDefaultableBeanDefinitions((BeanDefinitionRegistry) factory, factory);
	}

	private void registerMissingDefaultableBeanDefinitions(BeanDefinitionRegistry registry, ListableBeanFactory factory) {

		// see if any template definitions exist, which requires a converter, which requires a mapping context
		BeanDefinitionHolder[] templateBeans = getBeanDefinitionsOfType(registry, factory, CassandraOperations.class, true,
				true);

		if (templateBeans.length >= 1) {
			return;
		}

		// need a session & converter for the default template
		// see if an actual Session definition exists
		String sessionBeanName = findSessionBeanName(registry, factory);

		// see if any converter bean definitions exist, which requires a mapping context
		BeanDefinitionHolder[] converterBeans = getBeanDefinitionsOfType(registry, factory, MappingCassandraConverter.class,
				true, false);

		if (converterBeans.length > 1) {
			throw createAmbiguousBeansException(converterBeans.length, CassandraConverter.class, CassandraTemplate.class);
		}

		if (converterBeans.length == 1) {
			registerDefaultTemplate(registry, sessionBeanName, converterBeans[0].getBeanName());
			return;
		}

		// see if any mapping context bean definitions exist
		BeanDefinitionHolder[] contextBeans = getBeanDefinitionsOfType(registry, factory, CassandraMappingContext.class,
				true, false);

		if (contextBeans.length > 1) {
			// then throw, because we need to create a default converter, but we wouldn't know which mapping context to use
			throw createAmbiguousBeansException(contextBeans.length, MappingCassandraConverter.class,
					CassandraMappingContext.class);
		}

		// create the mapping context if necessary
		BeanDefinitionHolder contextBean = contextBeans.length == 1 ? contextBeans[0] : registerDefaultContext(registry);

		// create the default converter & template bean definitions
		BeanDefinitionHolder converter = registerDefaultConverter(registry, contextBean.getBeanName());
		registerDefaultTemplate(registry, sessionBeanName, converter.getBeanName());
	}

	private String findSessionBeanName(BeanDefinitionRegistry registry, ListableBeanFactory factory) {

		// first, search for any session and session factory beans
		BeanDefinitionHolder[] sessionBeans = getBeanDefinitionsOfType(registry, factory, Session.class, true, true);

		if (sessionBeans.length == 1) { // can't create default template -- none or multiple
			return sessionBeans[0].getBeanName();
		}

		throw createAmbiguousBeansException(sessionBeans.length, CassandraTemplate.class, Session.class,
				CassandraSessionFactoryBean.class);
	}

	private IllegalStateException createAmbiguousBeansException(int beanDefinitionCount, Class<?> defaultBeanType,
			Class<?>... types) {

		return new IllegalStateException(
				String.format("found %d beans of type%s [%s] - %s for creation of default [%s]", beanDefinitionCount,
						beanDefinitionCount == 1 ? "" : "s", StringUtils.collectionToCommaDelimitedString(getNames(types)),
						beanDefinitionCount == 0 ? "need exactly one" : "can't disambiguate", defaultBeanType.getName()));
	}

	private BeanDefinitionHolder registerDefaultContext(BeanDefinitionRegistry registry) {

		BeanDefinitionHolder contextBean = new BeanDefinitionHolder(
				BeanDefinitionBuilder.genericBeanDefinition(CassandraMappingContext.class).getBeanDefinition(),
				DefaultBeanNames.CONTEXT);

		registry.registerBeanDefinition(contextBean.getBeanName(), contextBean.getBeanDefinition());

		return contextBean;
	}

	private BeanDefinitionHolder registerDefaultConverter(BeanDefinitionRegistry registry, String contextBeanName) {

		BeanDefinition beanDefinition = BeanDefinitionBuilder //
				.genericBeanDefinition(MappingCassandraConverter.class) //
				.addConstructorArgReference(contextBeanName).getBeanDefinition();

		BeanDefinitionHolder converter = new BeanDefinitionHolder(beanDefinition, DefaultBeanNames.CONVERTER);
		registry.registerBeanDefinition(converter.getBeanName(), converter.getBeanDefinition());

		return converter;
	}

	private void registerDefaultTemplate(BeanDefinitionRegistry registry, String sessionBeanName,
			String converterBeanName) {

		BeanDefinition beanDefinition = BeanDefinitionBuilder.genericBeanDefinition(CassandraTemplate.class) //
				.addConstructorArgReference(sessionBeanName) //
				.addConstructorArgReference(converterBeanName) //
				.getBeanDefinition();

		BeanDefinitionHolder template = new BeanDefinitionHolder(beanDefinition, DefaultBeanNames.DATA_TEMPLATE);
		registry.registerBeanDefinition(template.getBeanName(), template.getBeanDefinition());
	}

	private Collection<String> getNames(Class<?>[] types) {

		List<String> names = new ArrayList<>();

		for (Class<?> type : types) {
			names.add(type.getName());
		}

		return names;
	}
}
