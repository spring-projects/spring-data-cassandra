/*
 * Copyright 2024 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import java.net.InetSocketAddress;
import java.util.Collections;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.Person;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.CassandraCustomConversions;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.SimpleUserTypeResolver;

/**
 * @author Christoph Strobl
 */
class LazyStartupConfigurationTest {

	@Test // GH-380
	void shouldDelayCqlSessionBeanInitializationTillFirstUsage() {

		GenericApplicationContext ctx = new AnnotationConfigApplicationContext(LazyStartupConfig.class);

		CassandraTemplate template = ctx.getBean(CassandraTemplate.class);
		assertThat(template).isNotNull();
		assertThatExceptionOfType(BeanCreationException.class).isThrownBy(() -> template.count(Person.class));
	}

	@Configuration
	static class LazyStartupConfig {

		@Lazy
		@Bean
		CqlSession cqlSession() {

			return CqlSession.builder()
					.addContactEndPoint(new DefaultEndPoint(InetSocketAddress.createUnresolved("127.0.0.2", 9042)))
					.withKeyspace("system")
					.build();
		}

		@Bean
		public SessionFactoryFactoryBean cassandraSessionFactory(CassandraConverter converter, @Lazy CqlSession cqlSession) {

			SessionFactoryFactoryBean session = new SessionFactoryFactoryBean();
			session.setSession(cqlSession);
			session.setConverter(converter);
			session.setSchemaAction(SchemaAction.NONE);
			return session;
		}

		@Bean
		public CassandraMappingContext cassandraMappingContext() {
			CassandraMappingContext context = new CassandraMappingContext();
			context.setSimpleTypeHolder(new CassandraCustomConversions(Collections.emptyList()).getSimpleTypeHolder());
			return context;
		}

		@Bean
		public CassandraConverter cassandraConverter(CassandraMappingContext mappingContext, @Lazy CqlSession cqlSession) {

			MappingCassandraConverter converter = new MappingCassandraConverter(mappingContext);
			converter.setCodecRegistry(() -> cqlSession.getContext().getCodecRegistry());
			converter.setCustomConversions(mappingContext.getCustomConversions());

			CqlIdentifier keyspace = CqlIdentifier.fromCql("system");
			converter.setUserTypeResolver(new SimpleUserTypeResolver(cqlSession::getMetadata, keyspace));
			return converter;
		}

		@Bean
		public CassandraTemplate cassandraTemplate(SessionFactory sessionFactory, CassandraConverter converter) {
			return new CassandraTemplate(sessionFactory, converter);
		}
	}
}
