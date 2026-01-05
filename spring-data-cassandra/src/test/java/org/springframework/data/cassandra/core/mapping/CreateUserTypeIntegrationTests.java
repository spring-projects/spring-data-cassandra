/*
 * Copyright 2017-present the original author or authors.
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

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.CassandraManagedTypes;
import org.springframework.data.cassandra.repository.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.repository.support.IntegrationTestConfig;
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

		@Override
		@Bean
		public CassandraManagedTypes cassandraManagedTypes() {
			return CassandraManagedTypes.fromIterable(Arrays.asList(Car.class, Engine.class, Manufacturer.class));
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
	private static class Car {

		@Id String id;
		Engine engine;

		public Car(String id, Engine engine) {
			this.id = id;
			this.engine = engine;
		}

		public String getId() {
			return this.id;
		}

		public Engine getEngine() {
			return this.engine;
		}
	}

	@UserDefinedType
	private static class Engine {
		Manufacturer manufacturer;
		List<Manufacturer> alternative;

		public Engine(Manufacturer manufacturer, List<Manufacturer> alternative) {
			this.manufacturer = manufacturer;
			this.alternative = alternative;
		}

		public Manufacturer getManufacturer() {
			return this.manufacturer;
		}

		public List<Manufacturer> getAlternative() {
			return this.alternative;
		}
	}

	@UserDefinedType
	private static class Manufacturer {
		String name;

		public Manufacturer(String name) {
			this.name = name;
		}

		public String getName() {
			return this.name;
		}
	}

}
