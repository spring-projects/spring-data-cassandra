/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.mapping;

import static org.assertj.core.api.Assertions.*;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.repository.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.repository.support.IntegrationTestConfig;
import org.springframework.data.convert.CustomConversions;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;

/**
 * Integration tests for creation of UDT types through {@link CassandraMappingContext}.
 *
 * @author Mark Paluch
 */
@SpringJUnitConfig
public class CreateUserTypeIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Configuration
	public static class Config extends IntegrationTestConfig {

		@Bean
		public CassandraMappingContext cassandraMapping() {

			CassandraMappingContext mappingContext = new CassandraMappingContext();

			mappingContext.setInitialEntitySet(new HashSet<>(Arrays.asList(Car.class, Engine.class, Manufacturer.class)));

			CustomConversions customConversions = customConversions();

			mappingContext.setCustomConversions(customConversions);
			mappingContext.setSimpleTypeHolder(customConversions.getSimpleTypeHolder());
			mappingContext.setUserTypeResolver(new SimpleUserTypeResolver(getRequiredSession()));

			return mappingContext;
		}
	}

	@Autowired CqlSession session;

	@Test // DATACASS-424
	void shouldCreateUserTypes() {

		KeyspaceMetadata keyspace = session.getMetadata().getKeyspace(session.getKeyspace().get()).get();

		assertThat(keyspace.getUserDefinedTypes()).containsKeys(CqlIdentifier.fromCql("engine"),
				CqlIdentifier.fromCql("manufacturer"));
	}

	@Table
	@Getter
	@AllArgsConstructor
	private static class Car {

		@Id String id;
		Engine engine;
	}

	@UserDefinedType
	@Getter
	@AllArgsConstructor
	private static class Engine {
		Manufacturer manufacturer;
		List<Manufacturer> alternative;
	}

	@UserDefinedType
	@Getter
	@AllArgsConstructor
	private static class Manufacturer {
		String name;
	}

}
