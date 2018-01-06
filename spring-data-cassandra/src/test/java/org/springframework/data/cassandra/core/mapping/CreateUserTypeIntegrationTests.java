/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.repository.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.repository.support.IntegrationTestConfig;
import org.springframework.data.convert.CustomConversions;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UserType;

/**
 * Integration tests for creation of UDT types through {@link CassandraMappingContext}.
 *
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class CreateUserTypeIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Configuration
	public static class Config extends IntegrationTestConfig {

		@Bean
		public CassandraMappingContext cassandraMapping() throws ClassNotFoundException {

			CassandraMappingContext mappingContext = new CassandraMappingContext();

			mappingContext.setInitialEntitySet(new HashSet<>(Arrays.asList(Car.class, Engine.class, Manufacturer.class)));

			CustomConversions customConversions = customConversions();

			mappingContext.setCustomConversions(customConversions);
			mappingContext.setSimpleTypeHolder(customConversions.getSimpleTypeHolder());
			mappingContext.setUserTypeResolver(new SimpleUserTypeResolver(cluster().getObject(), getKeyspaceName()));

			return mappingContext;
		}
	}

	@Autowired Session session;

	@Test // DATACASS-424
	public void shouldCreateUserTypes() {

		KeyspaceMetadata keyspace = session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace());

		Collection<UserType> userTypes = keyspace.getUserTypes();

		assertThat(userTypes).extracting("typeName").contains("engine", "manufacturer");
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
