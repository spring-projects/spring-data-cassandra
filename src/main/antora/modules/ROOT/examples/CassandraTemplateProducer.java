/*
 * Copyright 2020-2025 the original author or authors.
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
package org.springframework.data.cassandra.example;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.SimpleUserTypeResolver;

import com.datastax.oss.driver.api.core.CqlSession;

// tag::class[]
class CassandraTemplateProducer {

	@Produces
	@Singleton
	public CqlSession createSession() {
		return CqlSession.builder().withKeyspace("my-keyspace").build();
	}

	@Produces
	@ApplicationScoped
	public CassandraOperations createCassandraOperations(CqlSession session) {

		CassandraMappingContext mappingContext = new CassandraMappingContext();
		mappingContext.afterPropertiesSet();

		MappingCassandraConverter cassandraConverter = new MappingCassandraConverter(mappingContext);
		cassandraConverter.setUserTypeResolver(new SimpleUserTypeResolver(session));
		cassandraConverter.afterPropertiesSet();

		return new CassandraAdminTemplate(session, cassandraConverter);
	}

	public void close(@Disposes CqlSession session) {
		session.close();
	}
}
// end::class[]
