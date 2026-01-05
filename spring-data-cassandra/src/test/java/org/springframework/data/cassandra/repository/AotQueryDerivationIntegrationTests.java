/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.cassandra.repository;

import java.io.IOException;
import java.util.Set;

import org.springframework.beans.factory.config.PropertiesFactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.mapping.MapId;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.repository.aot.AotFragmentTestConfigurationSupport;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.repository.support.CassandraRepositoryFactoryBean;
import org.springframework.data.cassandra.repository.support.IntegrationTestConfig;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.support.PropertiesBasedNamedQueries;
import org.springframework.data.repository.core.support.RepositoryComposition;
import org.springframework.data.spel.ExtensionAwareEvaluationContextProvider;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * Integration test for {@link PersonRepository} using JavaConfig with mounted AOT-generated repository methods.
 *
 * @author Mark Paluch
 */
@SpringJUnitConfig(classes = AotQueryDerivationIntegrationTests.AotConfiguration.class)
class AotQueryDerivationIntegrationTests extends QueryDerivationIntegrationTests {

	@Configuration(proxyBeanMethods = false)
	@EnableCassandraRepositories(considerNestedRepositories = true,
			includeFilters = @ComponentScan.Filter(classes = { EmbeddedPersonRepository.class },
					type = FilterType.ASSIGNABLE_TYPE))
	static class AotConfiguration extends IntegrationTestConfig {

		@Override
		protected Set<Class<?>> getInitialEntitySet() {
			return Set.of(Person.class, PersonWithEmbedded.class);
		}

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.CREATE;
		}

		@Bean
		static AotFragmentTestConfigurationSupport aot() {
			return new AotFragmentTestConfigurationSupport(PersonRepository.class,
					QueryDerivationIntegrationTests.Config.class, false);
		}

		@Bean
		public QueryDerivationIntegrationTests.PersonRepository personRepository(ApplicationContext applicationContext,
				CassandraTemplate template) throws Exception {

			ExtensionAwareEvaluationContextProvider evaluationContextProvider = new ExtensionAwareEvaluationContextProvider(
					applicationContext);

			CassandraRepositoryFactoryBean<QueryDerivationIntegrationTests.PersonRepository, Person, MapId> factory = new CassandraRepositoryFactoryBean<>(
					QueryDerivationIntegrationTests.PersonRepository.class);
			factory.setCassandraTemplate(template);
			factory.setBeanFactory(applicationContext);

			factory.setRepositoryFragments(
					RepositoryComposition.RepositoryFragments.just(applicationContext.getBean("fragment")));

			factory.setNamedQueries(namedQueries());
			factory.setEvaluationContextProvider(evaluationContextProvider);
			factory.afterPropertiesSet();

			return factory.getObject();
		}

		private NamedQueries namedQueries() throws IOException {

			PropertiesFactoryBean factory = new PropertiesFactoryBean();
			factory.setLocation(new ClassPathResource("META-INF/cassandra-named-queries.properties"));
			factory.afterPropertiesSet();

			return new PropertiesBasedNamedQueries(factory.getObject());
		}
	}
}
